package reconcile_test

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/reconcile"
)

func setup(t *testing.T) *ledger.Ledger {
	t.Helper()
	lg, err := ledger.Open(filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	payments := []struct {
		ext, merch string
		amt        int64
	}{
		{"ok-1", "m1", 1000},
		{"mismatch-1", "m1", 2000},
		{"missing-stmt-1", "m2", 3000},
		{"dup-1", "m2", 4000},
	}
	for _, p := range payments {
		if _, err := lg.PostPayment(p.ext, p.merch, p.amt); err != nil {
			t.Fatal(err)
		}
	}
	return lg
}

func diffTypes(report *reconcile.Report) map[string][]string {
	out := map[string][]string{}
	for _, d := range report.Diffs {
		out[d.Type] = append(out[d.Type], d.ExternalID)
	}
	return out
}

// 场景：对账差异分类——单边缺失、金额不符、重复记录，全部标记无一遗漏。
func TestDiffClassification(t *testing.T) {
	t.Log("scenario: reconciliation diff classification")
	lg := setup(t)
	rec := reconcile.New(lg)

	statement := []reconcile.StatementLine{
		{LineID: "s1", ExternalID: "ok-1", Amount: 1000},
		{LineID: "s2", ExternalID: "mismatch-1", Amount: 2500}, // 金额不符
		{LineID: "s3", ExternalID: "dup-1", Amount: 4000},      // 重复（第 1 条）
		{LineID: "s4", ExternalID: "dup-1", Amount: 4000},      // 重复（第 2 条）
		{LineID: "s5", ExternalID: "ghost-1", Amount: 900},     // 账本缺失
		// missing-stmt-1 不出现在结算单：结算单缺失
	}
	report := rec.Reconcile(statement)
	types := diffTypes(report)

	expect := map[string]string{
		reconcile.DiffAmountMismatch:     "mismatch-1",
		reconcile.DiffDuplicate:          "dup-1",
		reconcile.DiffMissingInLedger:    "ghost-1",
		reconcile.DiffMissingInStatement: "missing-stmt-1",
	}
	for typ, extID := range expect {
		ids, ok := types[typ]
		if !ok || len(ids) == 0 || ids[0] != extID {
			t.Fatalf("diff type %s: got %v, want [%s]", typ, ids, extID)
		}
	}
	if report.Matched != 2 { // ok-1 与 dup-1 金额一致（重复另计差异）
		t.Fatalf("matched = %d, want 2", report.Matched)
	}
	// 覆盖性：结算单每一行、账本每一笔都必须被归类，不允许静默忽略
	covered := report.Matched*2 + len(report.Diffs)
	if covered < report.StatementLines || covered < report.LedgerTxns {
		t.Fatalf("coverage gap: matched=%d diffs=%d stmt=%d ledger=%d",
			report.Matched, len(report.Diffs), report.StatementLines, report.LedgerTxns)
	}
	t.Logf("result: all 4 diff types classified, matched=%d diffs=%d, no record silently dropped",
		report.Matched, len(report.Diffs))
}

// 场景：差异调整可追溯原始交易，且调整前后借贷总额守恒。
func TestAdjustmentConservation(t *testing.T) {
	t.Log("scenario: adjustment traceability and debit/credit conservation")
	lg := setup(t)
	rec := reconcile.New(lg)

	statement := []reconcile.StatementLine{
		{LineID: "s1", ExternalID: "ok-1", Amount: 1000},
		{LineID: "s2", ExternalID: "mismatch-1", Amount: 2500},
		{LineID: "s3", ExternalID: "dup-1", Amount: 4000},
		{LineID: "s4", ExternalID: "ghost-1", Amount: 900},
	}
	report := rec.Reconcile(statement)

	beforeD, beforeC := lg.Totals()
	if beforeD != beforeC {
		t.Fatalf("pre-adjust ledger unbalanced: %d/%d", beforeD, beforeC)
	}

	for i, d := range report.Diffs {
		adjustID := "adj-" + d.ExternalID
		var err error
		switch d.Type {
		case reconcile.DiffMissingInLedger:
			err = rec.Adjust(adjustID, d, "m1")
		case reconcile.DiffAmountMismatch:
			err = rec.Adjust(adjustID, d, "m1")
		default:
			continue
		}
		if err != nil {
			t.Fatalf("adjust %s: %v", d.ExternalID, err)
		}
		// 重复调整幂等
		if err := rec.Adjust(adjustID, d, "m1"); err != nil {
			t.Fatalf("re-adjust %s: %v", d.ExternalID, err)
		}
		// 可追溯：调整记录关联原始交易号
		adj, err := lg.GetTransaction(adjustID)
		if err != nil {
			t.Fatalf("adjustment txn %s not found: %v", adjustID, err)
		}
		if adj.MerchantID != "ADJ:"+d.ExternalID {
			t.Fatalf("adjustment ref = %s, want ADJ:%s", adj.MerchantID, d.ExternalID)
		}
		_ = i
	}

	afterD, afterC := lg.Totals()
	if afterD != afterC {
		t.Fatalf("post-adjust ledger unbalanced: %d/%d", afterD, afterC)
	}
	// 调整生效：mismatch-1 补差 500，ghost-1 补记 900
	if got := lg.Balance("acct:recv:m1"); got != 1000+2000+500+900 {
		t.Fatalf("m1 receivable = %d, want %d", got, 1000+2000+500+900)
	}
	t.Logf("result: adjustments applied idempotently, totals %d/%d balanced, traceable to source txns",
		afterD, afterC)
}

// 场景：完全一致时对账零差异。
func TestCleanReconcile(t *testing.T) {
	t.Log("scenario: clean reconcile with zero diffs")
	lg := setup(t)
	rec := reconcile.New(lg)
	statement := []reconcile.StatementLine{
		{LineID: "s1", ExternalID: "ok-1", Amount: 1000},
		{LineID: "s2", ExternalID: "mismatch-1", Amount: 2000},
		{LineID: "s3", ExternalID: "missing-stmt-1", Amount: 3000},
		{LineID: "s4", ExternalID: "dup-1", Amount: 4000},
	}
	report := rec.Reconcile(statement)
	if len(report.Diffs) != 0 || report.Matched != 4 {
		t.Fatalf("matched=%d diffs=%v, want 4/0", report.Matched, report.Diffs)
	}
	t.Log("result: 4/4 matched, zero diffs")
}
