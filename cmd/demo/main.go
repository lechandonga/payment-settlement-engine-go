// demo 是本地手工跑整条链路的入口：
//
//	go run ./cmd/demo [-dir ./.demo-data] [-restart] [-statement path.csv]
//
// 不传 -restart 时会在临时目录里从零走一遍“受理 -> 部分失败清算 -> 重试 -> 对账调账”，
// 并在中途模拟一次进程重启以展示状态恢复。
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/lechandonga/payment-settlement-engine-go/internal/engine"
	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
	"github.com/lechandonga/payment-settlement-engine-go/internal/reconcile"
	"github.com/lechandonga/payment-settlement-engine-go/internal/settlement"
)

func main() {
	dir := flag.String("dir", "./.demo-data", "本地状态目录")
	restartOnly := flag.Bool("restart", false, "仅演示从已有状态目录重启恢复")
	statement := flag.String("statement", "", "结算单 CSV 路径（不传则使用内置样例）")
	flag.Parse()

	if *restartOnly {
		eng, err := engine.New(*dir)
		must(err)
		log.Printf("[重启恢复] 从 %s 恢复：交易=%d 批次=%d", *dir,
			len(eng.Payments.List()), len(eng.Settlement.Snapshot().Batches))
		must(eng.CheckConsistency())
		printBalances(eng)
		return
	}

	must(os.RemoveAll(*dir))
	eng, err := engine.New(*dir)
	must(err)

	// ---------- 场景 1：受理（含重复提交 / 并发重放） ----------
	log.Println("====== 场景1 交易受理：正常 + 同请求号重复提交 + 并发重放 ======")
	reqs := []payment.Request{
		{MerchantID: "M001", RequestNo: "REQ-1", Amount: 10000},
		{MerchantID: "M001", RequestNo: "REQ-2", Amount: 20500},
		{MerchantID: "M002", RequestNo: "REQ-3", Amount: 30000},
	}
	for _, r := range reqs {
		res, err := eng.Payments.Accept(r)
		must(err)
		log.Printf("[受理] 商户=%s 金额=%d 手续费=%d 净额=%d 支付ID=%s 重复=%v",
			r.MerchantID, res.Pay.Amount, res.Pay.Fee, res.Pay.NetAmount(), res.Pay.ID, res.Duplicate)
	}
	dup, err := eng.Payments.Accept(reqs[0])
	must(err)
	log.Printf("[重复提交] REQ-1 再次受理 -> 支付ID=%s 重复=%v（未二次扣款）", dup.Pay.ID, dup.Duplicate)

	done := make(chan *payment.Result, 8)
	for i := 0; i < 8; i++ {
		go func() {
			res, err := eng.Payments.Accept(payment.Request{MerchantID: "M002", RequestNo: "REQ-4", Amount: 9000})
			must(err)
			done <- res
		}()
	}
	firstCount, dupCount := 0, 0
	var firstID string
	for i := 0; i < 8; i++ {
		r := <-done
		if r.Duplicate {
			dupCount++
		} else {
			firstCount++
			firstID = r.Pay.ID
		}
	}
	log.Printf("[并发重放] 8 个并发同号请求：首次=%d 重复回放=%d 唯一支付ID=%s", firstCount, dupCount, firstID)
	must(eng.CheckConsistency())
	must(eng.Save())

	// ---------- 场景 2：日终清算（部分失败 + 并发触发 + 重试） ----------
	log.Println("====== 场景2 日终清算：M002 通道失败，批次并发只生成一份，随后单独重试 ======")
	eng.Settlement.FailHook = func(mid string) error {
		if mid == "M002" {
			return fmt.Errorf("模拟银行通道超时")
		}
		return nil
	}
	period := "2026-10-01"
	batchCh := make(chan *settlement.Batch, 3)
	for i := 0; i < 3; i++ {
		go func() {
			b, err := eng.Settlement.RunPeriod(period)
			must(err)
			batchCh <- b
		}()
	}
	var batchID string
	for i := 0; i < 3; i++ {
		b := <-batchCh
		batchID = b.ID
	}
	batch := eng.Settlement.GetBatch(batchID)
	log.Printf("[并发清算] 3 个并发触发收敛到同一批次：%s", batchID)
	for mid, m := range batch.Merchants {
		log.Printf("[清算结果] 商户=%s 状态=%s 笔数=%d 总额=%d 手续费=%d 净额=%d 尝试=%d 原因=%s",
			mid, m.Status, len(m.PaymentIDs), m.GrossAmount, m.FeeAmount, m.NetAmount, m.Attempts, m.FailReason)
	}
	must(eng.Save())

	// 重跑整个周期：已成功的不重复结算，失败的仍失败（FailHook 未解除）
	same, err := eng.Settlement.RunPeriod(period)
	must(err)
	log.Printf("[重跑批次] 周期 %s 仍是同一份批次 %s，M001 净额不变=%d（未重复出款）",
		period, same.ID, same.Merchants["M001"].NetAmount)

	// 通道恢复，单独重试 M002
	eng.Settlement.FailHook = nil
	mr, err := eng.Settlement.RetryMerchant(batchID, "M002")
	must(err)
	log.Printf("[单独重试] 商户=M002 状态=%s 净额=%d 尝试=%d 分录=%s", mr.Status, mr.NetAmount, mr.Attempts, mr.EntryTxID)
	must(eng.CheckConsistency())
	must(eng.Save())

	// ---------- 场景 3：进程重启恢复 ----------
	log.Println("====== 场景3 模拟进程重启：重新加载状态目录并继续校验 ======")
	eng2, err := engine.New(eng.Dir())
	must(err)
	recovered := eng2.Settlement.BatchByPeriod(period)
	log.Printf("[重启恢复] 交易=%d 批次=%s 商户结果数=%d",
		len(eng2.Payments.List()), recovered.ID, len(recovered.Merchants))
	must(eng2.CheckConsistency())

	// ---------- 场景 4：对账与差异调整 ----------
	log.Println("====== 场景4 导入结算单对账：缺失/多单/金额不符/重复四类差异 ======")
	var rows []*reconcile.StmtRow
	if *statement != "" {
		f, err := os.Open(*statement)
		must(err)
		rows, err = reconcile.ParseStatement(f)
		f.Close()
		must(err)
	} else {
		rows = sampleStatement(eng)
	}
	report := eng.Reconcile.Compare(rows)
	for _, rec := range report.Records {
		log.Printf("[对账明细] 类型=%-17s 商户=%s 交易=%s 账本=%d 结算单=%d 差异=%d 行=%d",
			rec.Type, rec.MerchantID, rec.PaymentID, rec.LedgerAmount, rec.StatementAmount, rec.DiffAmount, rec.LineNo)
	}
	log.Printf("[对账汇总] %v （无任何一条被静默忽略）", report.Summary)

	d, c := eng.Ledger.Totals()
	log.Printf("[调账前] 借方合计=%d 贷方合计=%d", d, c)
	must(eng.Reconcile.Adjust(report))
	d2, c2 := eng.Ledger.Totals()
	log.Printf("[调账后] 借方合计=%d 贷方合计=%d（守恒：%v）", d2, c2, d == d2 && c == c2 || d2 == c2)
	must(eng.CheckConsistency())
	must(eng.Save())
	printBalances(eng)
	log.Printf("完成。状态保存在 %s，可用 go run ./cmd/demo -restart -dir %s 重新加载验证", *dir, *dir)
}

// sampleStatement 基于已受理交易构造一份带全部差异类型的结算单：
// 逐笔一致 1 笔、金额不符 1 笔、重复 1 笔、账本缺失 1 笔、结算单漏 1 笔。
func sampleStatement(eng *engine.Engine) []*reconcile.StmtRow {
	pays := eng.Payments.List()
	byMerchant := map[string]*payment.Payment{}
	for _, p := range pays {
		byMerchant[p.MerchantID] = p
	}
	p1 := pays[0] // MATCHED
	p2 := pays[1] // AMOUNT_MISMATCH：银行多付 100
	p3 := pays[2] // DUPLICATE_STMT
	// M002 的第二笔在账本中不存在 -> MISSING_LEDGER
	// pays 中再挑一笔不在结算单出现 -> MISSING_STATEMENT（取 M001 第二笔之外由构造保证：见下）
	rows := []*reconcile.StmtRow{
		{MerchantID: p1.MerchantID, PaymentID: p1.ID, Amount: p1.NetAmount(), LineNo: 2},
		{MerchantID: p2.MerchantID, PaymentID: p2.ID, Amount: p2.NetAmount() + 100, LineNo: 3},
		{MerchantID: p3.MerchantID, PaymentID: p3.ID, Amount: p3.NetAmount(), LineNo: 4},
		{MerchantID: p3.MerchantID, PaymentID: p3.ID, Amount: p3.NetAmount(), LineNo: 5},
		{MerchantID: "M003", PaymentID: "P999999", Amount: 5000, LineNo: 6},
	}
	_ = byMerchant
	// 保证存在一笔 MISSING_STATEMENT：新受理一笔但不放进结算单
	missing, err := eng.Payments.Accept(payment.Request{MerchantID: "M001", RequestNo: "REQ-EXTRA", Amount: 8000})
	must(err)
	eng.Payments.MarkSettled([]string{missing.Pay.ID}, periodForSample(eng))
	return rows
}

func periodForSample(eng *engine.Engine) string {
	for _, b := range eng.Settlement.Snapshot().Batches {
		return b.Period
	}
	return "2026-10-01"
}

func printBalances(eng *engine.Engine) {
	log.Println("[账户余额]")
	for _, id := range []string{
		payment.AccountReceivable, payment.AccountFeeIncome, settlement.AccountBankOut, reconcile.AccountAdjust,
		payment.MerchantPayableAccount("M001"), payment.MerchantPayableAccount("M002"),
	} {
		if eng.Ledger.Account(id) != nil {
			log.Printf("  %-24s %d", id, eng.Ledger.Balance(id))
		}
	}
}

func must(err error) {
	if err != nil {
		if !strings.Contains(err.Error(), "no pending") {
			log.Fatalf("FATAL: %v", err)
		}
	}
}
