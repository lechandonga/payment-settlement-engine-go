// pse 是本地演示入口：不接任何外部支付网关/银行，
// 在本地一次性走完 受理记账 -> 日终清算结算 -> 结算单对账 -> 差异调整 的整条链路。
//
// 用法：
//
//	go run ./cmd/pse [-dir ./data] [-period 2026-10-02]
//
// 状态持久化在 -dir 下，重复运行可观察幂等与重启恢复行为。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/lechandonga/payment-settlement-engine-go/internal/clearing"
	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/reconcile"
)

func main() {
	dir := flag.String("dir", "data", "本地状态目录")
	period := flag.String("period", time.Now().UTC().Format("2006-01-02"), "结算周期")
	flag.Parse()

	lg, err := ledger.Open(filepath.Join(*dir, "ledger.json"))
	if err != nil {
		log.Fatalf("open ledger: %v", err)
	}
	engine, err := clearing.Open(filepath.Join(*dir, "clearing.json"), lg, nil)
	if err != nil {
		log.Fatalf("open clearing engine: %v", err)
	}

	log.Printf("=== 1. accept & post (with duplicate demo) ===")
	payments := []struct {
		ext, merch string
		amt        int64
	}{
		{"order-1001", "m1", 129900},
		{"order-1002", "m1", 5800},
		{"order-1003", "m2", 66000},
		{"order-1004", "m2", 12000},
	}
	for _, p := range payments {
		res, err := lg.PostPayment(p.ext, p.merch, p.amt)
		if err != nil {
			log.Fatalf("post %s: %v", p.ext, err)
		}
		log.Printf("posted %s -> txn %s amount %d (duplicate=%v)", p.ext, res.Txn.ID, p.amt, res.Duplicate)
	}
	dup, err := lg.PostPayment("order-1001", "m1", 129900)
	if err != nil {
		log.Fatalf("duplicate post: %v", err)
	}
	log.Printf("duplicate order-1001 -> same txn %s (duplicate=%v)", dup.Txn.ID, dup.Duplicate)
	if _, err := lg.PostPayment("order-1001", "m1", 1); errors.Is(err, ledger.ErrDuplicate) {
		log.Printf("conflicting order-1001 rejected: %v", err)
	}
	debit, credit := lg.Totals()
	log.Printf("ledger totals: debit=%d credit=%d balanced=%v", debit, credit, debit == credit)

	log.Printf("=== 2. clearing & settlement (period=%s) ===", *period)
	batch, err := engine.RunBatch(*period)
	if err != nil {
		log.Fatalf("run batch: %v", err)
	}
	log.Printf("batch %s status=%s", batch.ID, batch.Status)
	for _, s := range batch.Settlements {
		log.Printf("  merchant %s net=%d txns=%d status=%s", s.MerchantID, s.NetAmount, len(s.TxnIDs), s.Status)
	}

	log.Printf("=== 3. reconcile (import local statement) ===")
	statement := []reconcile.StatementLine{
		{LineID: "s1", ExternalID: "order-1001", Amount: 129900},
		{LineID: "s2", ExternalID: "order-1002", Amount: 5800},
		{LineID: "s3", ExternalID: "order-1003", Amount: 66600},
		{LineID: "s4", ExternalID: "order-1003", Amount: 66600},
		{LineID: "s5", ExternalID: "order-9999", Amount: 3000},
	}
	rec := reconcile.New(lg)
	report := rec.Reconcile(statement)
	log.Printf("matched=%d diffs=%d (statement lines=%d, ledger txns=%d)",
		report.Matched, len(report.Diffs), report.StatementLines, report.LedgerTxns)
	for _, d := range report.Diffs {
		log.Printf("  diff [%s] %s ledger=%d statement=%d (%s)",
			d.Type, d.ExternalID, d.LedgerAmount, d.StatementAmount, d.Detail)
	}

	log.Printf("=== 4. adjustments (balance conservation check) ===")
	beforeD, beforeC := lg.Totals()
	for i, d := range report.Diffs {
		switch d.Type {
		case reconcile.DiffMissingInLedger:
			if err := rec.Adjust(fmt.Sprintf("adj-%d", i), d, "m1"); err != nil {
				log.Printf("  adjust %s failed: %v", d.ExternalID, err)
			} else {
				log.Printf("  adjusted %s: booked %d to ledger", d.ExternalID, d.StatementAmount)
			}
		case reconcile.DiffAmountMismatch:
			if err := rec.Adjust(fmt.Sprintf("adj-%d", i), d, "m2"); err != nil {
				log.Printf("  adjust %s failed: %v", d.ExternalID, err)
			} else {
				log.Printf("  adjusted %s: delta %d", d.ExternalID, d.StatementAmount-d.LedgerAmount)
			}
		default:
			log.Printf("  %s (%s) requires manual handling", d.ExternalID, d.Type)
		}
	}
	afterD, afterC := lg.Totals()
	log.Printf("totals before adjust: %d/%d, after: %d/%d, balanced=%v",
		beforeD, beforeC, afterD, afterC, afterD == afterC)
	log.Printf("done. state persisted under %s/", *dir)
}
