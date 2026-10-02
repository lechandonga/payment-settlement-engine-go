package engine

import (
	"errors"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
	"github.com/lechandonga/payment-settlement-engine-go/internal/settlement"
)

// 场景：受理 -> 部分商户清算失败 -> 落盘 -> 用全新 Engine 从目录恢复 -> 续做失败商户。
func TestRestartRecoveryContinuesUnfinished(t *testing.T) {
	dir := t.TempDir()

	eng, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []payment.Request{
		{MerchantID: "M001", RequestNo: "A", Amount: 10000},
		{MerchantID: "M002", RequestNo: "B", Amount: 20000},
	} {
		if _, err := eng.Payments.Accept(r); err != nil {
			t.Fatal(err)
		}
	}
	eng.Settlement.FailHook = func(mid string) error {
		if mid == "M002" {
			return errors.New("bank timeout")
		}
		return nil
	}
	b, err := eng.Settlement.RunPeriod("2026-10-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CheckConsistency(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Save(); err != nil {
		t.Fatal(err)
	}

	// === 进程重启：只拿到目录路径，重建一切 ===
	eng2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(eng2.Payments.List()) != 2 {
		t.Fatalf("payments lost: %d", len(eng2.Payments.List()))
	}
	rb := eng2.Settlement.BatchByPeriod("2026-10-01")
	if rb == nil || rb.ID != b.ID {
		t.Fatalf("batch lost: %+v", rb)
	}
	if rb.Merchants["M001"].Status != settlement.MerchantSuccess {
		t.Fatal("completed merchant regressed after restart")
	}
	if rb.Merchants["M002"].Status != settlement.MerchantFailed {
		t.Fatal("failed merchant state lost after restart")
	}
	if err := eng2.CheckConsistency(); err != nil {
		t.Fatal(err)
	}

	// 重启后原失败请求号仍然幂等
	res, err := eng2.Payments.Accept(payment.Request{MerchantID: "M001", RequestNo: "A", Amount: 10000})
	if err != nil || !res.Duplicate {
		t.Fatalf("idempotency lost after restart: %+v %v", res, err)
	}

	// 继续推进未完成部分，且不会重复结算 M001
	m1Net := rb.Merchants["M001"].NetAmount
	mr, err := eng2.Settlement.RetryMerchant(b.ID, "M002")
	if err != nil {
		t.Fatal(err)
	}
	if mr.Status != settlement.MerchantSuccess {
		t.Fatalf("resume failed: %+v", mr)
	}
	if eng2.Settlement.BatchByPeriod("2026-10-01").Merchants["M001"].NetAmount != m1Net {
		t.Fatal("M001 resettled after restart")
	}
	if err := eng2.CheckConsistency(); err != nil {
		t.Fatal(err)
	}
	if err := eng2.Save(); err != nil {
		t.Fatal(err)
	}

	// 再次重启，全部完成且守恒
	eng3, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !eng3.Settlement.BatchByPeriod("2026-10-01").AllDone() {
		t.Fatal("batch not complete after second restart")
	}
	if err := eng3.CheckConsistency(); err != nil {
		t.Fatal(err)
	}
}
