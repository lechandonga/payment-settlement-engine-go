package payment

import (
	"errors"
	"sync"
	"testing"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
)

func newSvc() (*Service, *ledger.Ledger) {
	lg := ledger.New()
	return NewService(lg), lg
}

func TestAcceptFeeAndEntries(t *testing.T) {
	s, lg := newSvc()
	res, err := s.Accept(Request{MerchantID: "M1", RequestNo: "R1", Amount: 10000})
	if err != nil {
		t.Fatal(err)
	}
	p := res.Pay
	if p.Fee != 60 || p.NetAmount() != 9940 {
		t.Fatalf("fee math wrong: fee=%d net=%d", p.Fee, p.NetAmount())
	}
	// 账户余额必须与分录汇总一致
	if lg.Balance(AccountReceivable) != 10000 {
		t.Fatalf("receivable=%d", lg.Balance(AccountReceivable))
	}
	if lg.Balance(MerchantPayableAccount("M1")) != -9940 {
		t.Fatalf("merchant payable=%d", lg.Balance(MerchantPayableAccount("M1")))
	}
	if lg.Balance(AccountFeeIncome) != -60 {
		t.Fatalf("fee income=%d", lg.Balance(AccountFeeIncome))
	}
	if d, c := lg.Totals(); d != c {
		t.Fatalf("ledger not balanced %d/%d", d, c)
	}
}

func TestDuplicateSubmissionReturnsFirstResult(t *testing.T) {
	s, lg := newSvc()
	first, err := s.Accept(Request{MerchantID: "M1", RequestNo: "R1", Amount: 10000})
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Accept(Request{MerchantID: "M1", RequestNo: "R1", Amount: 10000})
	if err != nil || !again.Duplicate || again.Pay.ID != first.Pay.ID {
		t.Fatalf("duplicate handling wrong: %+v err=%v", again, err)
	}
	if len(s.List()) != 1 {
		t.Fatalf("duplicate payment created: %d", len(s.List()))
	}
	// 同请求号不同金额必须明确拒绝
	if _, err := s.Accept(Request{MerchantID: "M1", RequestNo: "R1", Amount: 999}); !errors.Is(err, ErrRequestReplay) {
		t.Fatalf("expect ErrRequestReplay, got %v", err)
	}
	// 余额未变 = 没有重复扣款
	if lg.Balance(AccountReceivable) != 10000 {
		t.Fatalf("double charge: %d", lg.Balance(AccountReceivable))
	}
}

func TestConcurrentReplayIdempotent(t *testing.T) {
	s, _ := newSvc()
	const n = 50
	var wg sync.WaitGroup
	ids := make(chan string, n)
	dups := make(chan bool, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := s.Accept(Request{MerchantID: "M9", RequestNo: "HOT", Amount: 7777})
			if err != nil {
				t.Errorf("accept: %v", err)
				return
			}
			ids <- res.Pay.ID
			dups <- res.Duplicate
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	close(dups)
	firstN, dupN := 0, 0
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	for d := range dups {
		if d {
			dupN++
		} else {
			firstN++
		}
	}
	if firstN != 1 || dupN != n-1 || len(unique) != 1 {
		t.Fatalf("first=%d dup=%d unique=%d", firstN, dupN, len(unique))
	}
	if len(s.List()) != 1 {
		t.Fatalf("expected exactly 1 payment, got %d", len(s.List()))
	}
}

func TestIdempotencyAcrossReload(t *testing.T) {
	s, _ := newSvc()
	if _, err := s.Accept(Request{MerchantID: "M1", RequestNo: "R1", Amount: 500}); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	s2, _ := newSvc()
	s2.Load(snap)
	res, err := s2.Accept(Request{MerchantID: "M1", RequestNo: "R1", Amount: 500})
	if err != nil || !res.Duplicate {
		t.Fatalf("reload idempotency failed: %+v %v", res, err)
	}
}
