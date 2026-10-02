package reconcile

import (
	"strings"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
	"github.com/lechandonga/payment-settlement-engine-go/internal/settlement"
)

type fixture struct {
	lg   *ledger.Ledger
	pay  *payment.Service
	st   *settlement.Service
	svc  *Service
	pays map[string]*payment.Payment
}

func setup(t *testing.T) *fixture {
	t.Helper()
	lg := ledger.New()
	pay := payment.NewService(lg)
	st := settlement.NewService(lg, pay)
	svc := NewService(lg, pay, st)

	f := &fixture{lg: lg, pay: pay, st: st, svc: svc, pays: map[string]*payment.Payment{}}
	for _, r := range []payment.Request{
		{MerchantID: "M1", RequestNo: "1", Amount: 10000},
		{MerchantID: "M1", RequestNo: "2", Amount: 20000},
		{MerchantID: "M2", RequestNo: "3", Amount: 30000},
	} {
		res, err := pay.Accept(r)
		if err != nil {
			t.Fatal(err)
		}
		f.pays[r.MerchantID+":"+r.RequestNo] = res.Pay
	}
	if _, err := st.RunPeriod("P"); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestCompareClassifiesAllDifferences(t *testing.T) {
	f := setup(t)
	p1 := f.pays["M1:1"]
	p2 := f.pays["M1:2"]
	p3 := f.pays["M2:3"]

	rows := []*StmtRow{
		{MerchantID: "M1", PaymentID: p1.ID, Amount: p1.NetAmount(), LineNo: 2},       // 匹配
		{MerchantID: "M1", PaymentID: p2.ID, Amount: p2.NetAmount() + 100, LineNo: 3}, // 金额不符（多100）
		{MerchantID: "M2", PaymentID: p3.ID, Amount: p3.NetAmount(), LineNo: 4},       // 匹配
		{MerchantID: "M2", PaymentID: p3.ID, Amount: p3.NetAmount(), LineNo: 5},       // 结算单重复
		{MerchantID: "MX", PaymentID: "P999999", Amount: 5000, LineNo: 6},             // 账本缺失
	}
	// p2 在账本有但故意不放进结算单？不，上面放了。另造一笔已结算但结算单漏掉的：
	extra, err := f.pay.Accept(payment.Request{MerchantID: "M1", RequestNo: "9", Amount: 8000})
	if err != nil {
		t.Fatal(err)
	}
	f.pay.MarkSettled([]string{extra.Pay.ID}, "P")

	report := f.svc.Compare(rows)
	want := map[string]int{
		TypeMatched: 2, TypeAmountMismatch: 1, TypeDuplicateInStmt: 1,
		TypeMissingLedger: 1, TypeMissingStatement: 1,
	}
	for typ, n := range want {
		if report.Summary[typ] != n {
			t.Fatalf("%s: want %d got %d (summary=%v)", typ, n, report.Summary[typ], report.Summary)
		}
	}
	if len(report.Records) != 6 {
		t.Fatalf("every difference must be reported, got %d", len(report.Records))
	}

	dBefore, cBefore := f.lg.Totals()
	if err := f.svc.Adjust(report); err != nil {
		t.Fatal(err)
	}
	dAfter, cAfter := f.lg.Totals()
	if dAfter != cAfter {
		t.Fatalf("adjustment unbalanced: %d %d", dAfter, cAfter)
	}
	if dAfter-dBefore != cAfter-cBefore {
		t.Fatalf("debit/credit delta unequal")
	}
	// 每条差异都留下了可追溯的调整分录或明确无需调账的说明
	for _, rec := range report.Records {
		if rec.Type != TypeMatched && rec.Type != TypeDuplicateInStmt && rec.AdjustTxID == "" {
			t.Fatalf("untraced adjustment: %+v", rec)
		}
		if rec.Type != TypeMatched {
			for _, e := range f.lg.EntriesByTx(rec.AdjustTxID) {
				if !strings.Contains(e.Remark, rec.PaymentID) {
					t.Fatalf("adjust entry not traceable: %+v", e)
				}
			}
		}
	}

	// 调账幂等：再来一遍不能产生新分录
	if err := f.svc.Adjust(report); err != nil {
		t.Fatal(err)
	}
	d2, c2 := f.lg.Totals()
	if d2 != dAfter || c2 != cAfter {
		t.Fatalf("adjustment not idempotent")
	}
}

func TestParseStatementCSV(t *testing.T) {
	csv := "merchant_id,payment_id,net_amount\nM1,P000001,9940\nM2,P000002,100\n"
	rows, err := ParseStatement(strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Amount != 9940 || rows[0].LineNo != 2 {
		t.Fatalf("parse wrong: %+v", rows)
	}
}
