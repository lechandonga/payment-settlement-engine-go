package settlement

import (
	"errors"
	"sync"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
)

func setup(t *testing.T) (*Service, *payment.Service, *ledger.Ledger) {
	t.Helper()
	lg := ledger.New()
	pay := payment.NewService(lg)
	st := NewService(lg, pay)
	reqs := []payment.Request{
		{MerchantID: "M001", RequestNo: "A", Amount: 10000},
		{MerchantID: "M001", RequestNo: "B", Amount: 20000},
		{MerchantID: "M002", RequestNo: "C", Amount: 30000},
	}
	for _, r := range reqs {
		if _, err := pay.Accept(r); err != nil {
			t.Fatal(err)
		}
	}
	return st, pay, lg
}

func TestConcurrentBatchSingleInstanceAndPartialFailure(t *testing.T) {
	st, _, _ := setup(t)
	st.FailHook = func(mid string) error {
		if mid == "M002" {
			return errors.New("channel down")
		}
		return nil
	}

	const n = 10
	var wg sync.WaitGroup
	ids := make(chan string, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			b, err := st.RunPeriod("2026-10-01")
			if err != nil {
				t.Errorf("run: %v", err)
				return
			}
			ids <- b.ID
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("multiple batches: %s vs %s", first, id)
		}
	}

	batch := st.GetBatch(first)
	if batch.Merchants["M001"].Status != MerchantSuccess {
		t.Fatalf("M001 should succeed: %+v", batch.Merchants["M001"])
	}
	if batch.Merchants["M002"].Status != MerchantFailed {
		t.Fatalf("M002 should fail: %+v", batch.Merchants["M002"])
	}

	m1 := batch.Merchants["M001"]
	if m1.GrossAmount != 30000 || m1.FeeAmount != 180 || m1.NetAmount != 29820 {
		t.Fatalf("M001 totals wrong: %+v", m1)
	}
}

func TestRerunDoesNotResettleSuccess(t *testing.T) {
	st, pay, lg := setup(t)
	failM2 := true
	st.FailHook = func(mid string) error {
		if mid == "M002" && failM2 {
			return errors.New("channel down")
		}
		return nil
	}
	b1, err := st.RunPeriod("P")
	if err != nil {
		t.Fatal(err)
	}
	netM1Before := b1.Merchants["M001"].NetAmount

	// 再次触发同周期：拿到同批次，M001 净额、出款账户都不应变化
	b2, err := st.RunPeriod("P")
	if err != nil || b1.ID != b2.ID {
		t.Fatalf("rerun batch wrong: %s %s err=%v", b1.ID, b2.ID, err)
	}
	if b2.Merchants["M001"].NetAmount != netM1Before {
		t.Fatal("M001 resettled on rerun")
	}
	outBefore := lg.Balance(AccountBankOut)

	// 通道恢复，只重试失败的 M002
	failM2 = false
	mr, err := st.RetryMerchant(b1.ID, "M002")
	if err != nil {
		t.Fatal(err)
	}
	if mr.Status != MerchantSuccess || mr.Attempts != 2 {
		t.Fatalf("retry failed: %+v", mr)
	}
	if len(pay.PendingSettlement()) != 0 {
		t.Fatalf("still pending after retry")
	}
	// M001 没被重复出款；总出款只增加了 M002 的净额
	if lg.Balance(AccountBankOut) != outBefore-mr.NetAmount {
		t.Fatalf("bank out wrong: %d vs %d", lg.Balance(AccountBankOut), outBefore-mr.NetAmount)
	}

	// 重试已成功的商户是幂等的
	again, err := st.RetryMerchant(b1.ID, "M001")
	if err != nil || again.Attempts != 1 {
		t.Fatalf("retry success not idempotent: %+v %v", again, err)
	}

	if d, c := lg.Totals(); d != c {
		t.Fatalf("unbalanced: %d %d", d, c)
	}
}

func TestSnapshotRestartKeepsSucceeded(t *testing.T) {
	st, _, lg := setup(t)
	st.FailHook = func(mid string) error {
		if mid == "M002" {
			return errors.New("channel down")
		}
		return nil
	}
	b, _ := st.RunPeriod("P")

	// 模拟重启：新的清算服务实例从快照恢复，M001 仍成功、M002 仍失败可继续推进
	st2 := NewService(lg, st.pay)
	st2.Load(st.Snapshot())

	rb := st2.GetBatch(b.ID)
	if rb.Merchants["M001"].Status != MerchantSuccess || rb.Merchants["M002"].Status != MerchantFailed {
		t.Fatalf("state lost across restart: %+v", rb.Merchants)
	}
	mr, err := st2.RetryMerchant(b.ID, "M002")
	if err != nil || mr.Status != MerchantSuccess {
		t.Fatalf("resume failed: %+v %v", mr, err)
	}
}
