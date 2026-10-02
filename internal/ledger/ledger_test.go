package ledger_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
)

func openTemp(t *testing.T) (*ledger.Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.json")
	lg, err := ledger.Open(path)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	return lg, path
}

// 场景：重复提交同一外部请求号，只能入账一次，且拿到与首次一致的结果。
func TestDuplicateSubmitReturnsSameResult(t *testing.T) {
	t.Log("scenario: duplicate submit / idempotent posting")
	lg, _ := openTemp(t)

	first, err := lg.PostPayment("ext-1", "m1", 1000)
	if err != nil {
		t.Fatalf("first post: %v", err)
	}
	for i := 0; i < 5; i++ {
		dup, err := lg.PostPayment("ext-1", "m1", 1000)
		if err != nil {
			t.Fatalf("dup post %d: %v", i, err)
		}
		if !dup.Duplicate || dup.Txn.ID != first.Txn.ID {
			t.Fatalf("dup %d: got txn %s duplicate=%v, want %s", i, dup.Txn.ID, dup.Duplicate, first.Txn.ID)
		}
	}
	if got := lg.Balance("acct:recv:m1"); got != 1000 {
		t.Fatalf("balance = %d, want 1000 (no double posting)", got)
	}
	d, c := lg.Totals()
	if d != c {
		t.Fatalf("ledger unbalanced: debit=%d credit=%d", d, c)
	}
	t.Log("result: 5 duplicate submits -> single posting, identical result, ledger balanced")
}

// 场景：同号不同金额/商户，必须明确拒绝。
func TestConflictingDuplicateRejected(t *testing.T) {
	t.Log("scenario: conflicting duplicate rejected")
	lg, _ := openTemp(t)
	if _, err := lg.PostPayment("ext-2", "m1", 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := lg.PostPayment("ext-2", "m1", 999); !errors.Is(err, ledger.ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	if _, err := lg.PostPayment("ext-2", "m2", 1000); !errors.Is(err, ledger.ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	t.Log("result: mismatched amount/merchant both rejected with ErrDuplicate")
}

// 场景：并发重放同一请求号（超时重试/并发重放），只能入账一次。
func TestConcurrentReplaySinglePosting(t *testing.T) {
	t.Log("scenario: concurrent replay of same external id")
	lg, _ := openTemp(t)

	const workers = 64
	var wg sync.WaitGroup
	ids := make([]string, workers)
	dups := make([]bool, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := lg.PostPayment("ext-race", "m1", 500)
			if err == nil {
				ids[i] = res.Txn.ID
				dups[i] = res.Duplicate
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()

	var firstID string
	nonDup := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d err: %v", i, errs[i])
		}
		if !dups[i] {
			nonDup++
			firstID = ids[i]
		}
	}
	if nonDup != 1 {
		t.Fatalf("non-duplicate results = %d, want exactly 1", nonDup)
	}
	for i := 0; i < workers; i++ {
		if ids[i] != firstID {
			t.Fatalf("worker %d got txn %s, want %s", i, ids[i], firstID)
		}
	}
	if got := lg.Balance("acct:recv:m1"); got != 500 {
		t.Fatalf("balance = %d, want 500 (exactly one posting)", got)
	}
	d, c := lg.Totals()
	if d != c {
		t.Fatalf("ledger unbalanced: debit=%d credit=%d", d, c)
	}
	t.Logf("result: %d concurrent replays -> 1 posting, all callers got txn %s", workers, firstID)
}

// 场景：多账户并发记账，任意时刻借贷守恒。
func TestConcurrentMixedPostingsBalanced(t *testing.T) {
	t.Log("scenario: concurrent mixed postings keep ledger balanced")
	lg, _ := openTemp(t)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				ext := string(rune('a'+w)) + "-" + string(rune('0'+i%10)) + "-" + string(rune('0'+i/10))
				if _, err := lg.PostPayment(ext, "m"+string(rune('1'+w%3)), int64(100+i)); err != nil {
					t.Errorf("post %s: %v", ext, err)
				}
			}
		}(w)
	}
	wg.Wait()
	d, c := lg.Totals()
	if d != c {
		t.Fatalf("ledger unbalanced: debit=%d credit=%d", d, c)
	}
	t.Logf("result: 400 concurrent postings, totals debit=%d credit=%d balanced", d, c)
}

// 场景：进程重启后账本状态恢复，已入账交易不丢失、不重复。
func TestRestartRecovery(t *testing.T) {
	t.Log("scenario: restart recovery of ledger state")
	lg, path := openTemp(t)
	res, err := lg.PostPayment("ext-restart", "m1", 777)
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := ledger.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	dup, err := reopened.PostPayment("ext-restart", "m1", 777)
	if err != nil {
		t.Fatal(err)
	}
	if !dup.Duplicate || dup.Txn.ID != res.Txn.ID {
		t.Fatalf("after restart: duplicate=%v id=%s, want true/%s", dup.Duplicate, dup.Txn.ID, res.Txn.ID)
	}
	if got := reopened.Balance("acct:recv:m1"); got != 777 {
		t.Fatalf("balance after restart = %d, want 777", got)
	}
	t.Log("result: state recovered after restart, replay still idempotent")
}
