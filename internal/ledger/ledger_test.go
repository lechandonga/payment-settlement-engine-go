package ledger

import (
	"errors"
	"testing"
)

func TestPostBalancedAndBalance(t *testing.T) {
	l := New()
	l.EnsureAccount("A", "资产", Asset, "CNY")
	l.EnsureAccount("B", "负债", Liability, "CNY")

	if _, err := l.Post("T1", []Posting{
		{AccountID: "A", Amount: 100, Debit: true},
		{AccountID: "B", Amount: 90, Debit: false},
	}, "不平"); !errors.Is(err, ErrUnbalanced) {
		t.Fatalf("expect ErrUnbalanced, got %v", err)
	}

	if _, err := l.Post("T1", []Posting{
		{AccountID: "A", Amount: 100, Debit: true},
		{AccountID: "B", Amount: 100, Debit: false},
	}, ""); err != nil {
		t.Fatalf("post: %v", err)
	}
	// 同一 TxID 再入账必须被拒绝（业务幂等的底层保证）
	if _, err := l.Post("T1", []Posting{
		{AccountID: "A", Amount: 1, Debit: true},
		{AccountID: "B", Amount: 1, Debit: false},
	}, ""); !errors.Is(err, ErrTxAlreadyPosted) {
		t.Fatalf("expect ErrTxAlreadyPosted, got %v", err)
	}
	if l.Balance("A") != 100 || l.Balance("B") != -100 {
		t.Fatalf("balances wrong: %d %d", l.Balance("A"), l.Balance("B"))
	}
	d, c := l.Totals()
	if d != 100 || c != 100 {
		t.Fatalf("totals: %d %d", d, c)
	}

	// 负债账户不能出现借方余额（防止重复结算透支）
	if _, err := l.Post("T2", []Posting{
		{AccountID: "B", Amount: 200, Debit: true},
		{AccountID: "A", Amount: 200, Debit: false},
	}, "透支"); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("expect ErrInsufficient, got %v", err)
	}
}

func TestSnapshotReload(t *testing.T) {
	l := New()
	l.EnsureAccount("A", "a", Asset, "CNY")
	l.EnsureAccount("B", "b", Liability, "CNY")
	if _, err := l.Post("T1", []Posting{
		{AccountID: "A", Amount: 100, Debit: true},
		{AccountID: "B", Amount: 100, Debit: false},
	}, ""); err != nil {
		t.Fatal(err)
	}
	s := l.Snapshot()
	l2 := New()
	l2.Load(s)
	if l2.Balance("A") != 100 {
		t.Fatalf("reload balance wrong: %d", l2.Balance("A"))
	}
}
