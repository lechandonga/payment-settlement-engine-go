package clearing_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/clearing"
	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
)

func setup(t *testing.T, hook clearing.SettleHook) (*ledger.Ledger, *clearing.Engine, string, string) {
	t.Helper()
	dir := t.TempDir()
	ledgerPath := filepath.Join(dir, "ledger.json")
	enginePath := filepath.Join(dir, "clearing.json")
	lg, err := ledger.Open(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := clearing.Open(enginePath, lg, hook)
	if err != nil {
		t.Fatal(err)
	}
	return lg, engine, ledgerPath, enginePath
}

func seed(t *testing.T, lg *ledger.Ledger) {
	t.Helper()
	payments := []struct {
		ext, merch string
		amt        int64
	}{
		{"o1", "m1", 1000},
		{"o2", "m1", 2000},
		{"o3", "m2", 3000},
		{"o4", "m3", 4000},
	}
	for _, p := range payments {
		if _, err := lg.PostPayment(p.ext, p.merch, p.amt); err != nil {
			t.Fatal(err)
		}
	}
}

// 场景：同一批次并发触发，最终只产生一份有效结算。
func TestConcurrentBatchTriggerSingleEffective(t *testing.T) {
	t.Log("scenario: concurrent batch trigger -> single effective batch")
	lg, engine, _, _ := setup(t, nil)
	seed(t, lg)

	const workers = 16
	var wg sync.WaitGroup
	batches := make([]*clearing.Batch, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, err := engine.RunBatch("2026-10-02")
			if err != nil {
				t.Errorf("run batch: %v", err)
				return
			}
			batches[i] = b
		}(i)
	}
	wg.Wait()

	for i, b := range batches {
		if b == nil || b.ID != "batch-2026-10-02" {
			t.Fatalf("worker %d got batch %+v, want batch-2026-10-02", i, b)
		}
		if b.Status != clearing.BatchCompleted {
			t.Fatalf("worker %d batch status %s, want COMPLETED", i, b.Status)
		}
	}
	if got := len(engine.Batches()); got != 1 {
		t.Fatalf("batch count = %d, want 1", got)
	}
	for m, want := range map[string]int64{"m1": 3000, "m2": 3000, "m3": 4000} {
		if got := lg.Balance("acct:pay:" + m); got != -want {
			t.Fatalf("merchant %s payable balance = %d, want %d", m, got, -want)
		}
	}
	d, c := lg.Totals()
	if d != c {
		t.Fatalf("ledger unbalanced: %d/%d", d, c)
	}
	t.Logf("result: %d concurrent triggers -> 1 batch, each merchant settled exactly once", workers)
}

// 场景：批次内部分商户失败，重跑不重复结算已成功部分，失败的可单独重试。
func TestPartialFailureAndRerun(t *testing.T) {
	t.Log("scenario: partial settlement failure, rerun and targeted retry")
	failM2 := true
	var mu sync.Mutex
	calls := map[string]int{}
	hook := func(lg *ledger.Ledger) clearing.SettleHook {
		return func(batchID, merchantID string, amount int64, txnIDs []string) error {
			if merchantID == "m2" && failM2 {
				return errors.New("simulated payout channel error")
			}
			mu.Lock()
			calls[merchantID]++
			mu.Unlock()
			return lg.SettleEntries(batchID, merchantID, amount, txnIDs)
		}
	}
	lg, engine, _, _ := setup(t, nil)
	seed(t, lg)

	dir := t.TempDir()
	engine, err := clearing.Open(filepath.Join(dir, "clearing.json"), lg, hook(lg))
	if err != nil {
		t.Fatal(err)
	}

	batch, err := engine.RunBatch("2026-10-03")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != clearing.BatchPartial {
		t.Fatalf("batch status = %s, want PARTIAL", batch.Status)
	}
	if s := batch.Settlements["m2"]; s.Status != clearing.SettleFailed || s.FailReason == "" {
		t.Fatalf("m2 settlement = %+v, want FAILED with reason", s)
	}
	for _, m := range []string{"m1", "m3"} {
		if s := batch.Settlements[m]; s.Status != clearing.SettleSuccess {
			t.Fatalf("%s settlement = %s, want SUCCESS", m, s.Status)
		}
	}
	t.Logf("first run: m1/m3 SUCCESS, m2 FAILED (%s)", batch.Settlements["m2"].FailReason)

	if _, err := engine.RunBatch("2026-10-03"); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"m1", "m3"} {
		if calls[m] != 1 {
			t.Fatalf("%s settled %d times, want exactly 1", m, calls[m])
		}
	}
	t.Log("rerun: successful merchants not re-settled")

	failM2 = false
	s, err := engine.RetryMerchant("batch-2026-10-03", "m2")
	if err != nil {
		t.Fatal(err)
	}
	if s.Status != clearing.SettleSuccess {
		t.Fatalf("retry m2 status = %s, want SUCCESS", s.Status)
	}
	final, _ := engine.GetBatch("batch-2026-10-03")
	if final.Status != clearing.BatchCompleted {
		t.Fatalf("final batch status = %s, want COMPLETED", final.Status)
	}
	if got := lg.Balance("acct:pay:m2"); got != -3000 {
		t.Fatalf("m2 payable = %d, want -3000 (settled exactly once)", got)
	}
	d, c := lg.Totals()
	if d != c {
		t.Fatalf("ledger unbalanced: %d/%d", d, c)
	}
	t.Log("result: targeted retry succeeded, batch COMPLETED, no double settlement")
}

// 场景：进程重启后已完成的结算不回退，未完成的可继续推进。
func TestRestartRecovery(t *testing.T) {
	t.Log("scenario: restart recovery of clearing state")
	lg, engine, ledgerPath, enginePath := setup(t, nil)
	seed(t, lg)

	batch, err := engine.RunBatch("2026-10-04")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != clearing.BatchCompleted {
		t.Fatalf("batch status = %s, want COMPLETED", batch.Status)
	}

	lg2, err := ledger.Open(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	engine2, err := clearing.Open(enginePath, lg2, nil)
	if err != nil {
		t.Fatal(err)
	}
	b2, ok := engine2.GetBatch("batch-2026-10-04")
	if !ok || b2.Status != clearing.BatchCompleted {
		t.Fatalf("after restart: batch = %+v ok=%v, want COMPLETED", b2, ok)
	}
	again, err := engine2.RunBatch("2026-10-04")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != batch.ID || len(engine2.Batches()) != 1 {
		t.Fatalf("rerun after restart produced extra batch: %+v", again)
	}
	if got := lg2.Balance("acct:pay:m1"); got != -3000 {
		t.Fatalf("m1 payable after restart = %d, want -3000", got)
	}
	if _, err := lg2.PostPayment("o5", "m1", 500); err != nil {
		t.Fatal(err)
	}
	nb, err := engine2.RunBatch("2026-10-05")
	if err != nil {
		t.Fatal(err)
	}
	if nb.Status != clearing.BatchCompleted || nb.Settlements["m1"].NetAmount != 500 {
		t.Fatalf("next-period batch = %+v", nb)
	}
	t.Log("result: completed batch intact after restart, new txns cleared in next period")
}
