package ledger

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// 金额一律使用最小货币单位（如人民币“分”）的 int64，全程不做浮点运算。

type AccountType string

const (
	Asset     AccountType = "ASSET"     // 资产
	Liability AccountType = "LIABILITY" // 负债
	Income    AccountType = "INCOME"    // 收入
)

type Account struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	Type     AccountType `json:"type"`
	Currency string      `json:"currency"`
}

// Entry 是一条明细分录。同一业务 TxID 下的所有分录借贷必须相等。
type Entry struct {
	ID        string `json:"id"`
	TxID      string `json:"tx_id"`
	AccountID string `json:"account_id"`
	Debit     int64  `json:"debit"`
	Credit    int64  `json:"credit"`
	Remark    string `json:"remark"`
	CreatedAt int64  `json:"created_at"`
}

type Posting struct {
	AccountID string
	Amount    int64 // 正数；Debit 决定方向
	Debit     bool  // true=借方 false=贷方
}

type Snapshot struct {
	Accounts []*Account `json:"accounts"`
	Entries  []*Entry   `json:"entries"`
	Counter  int64      `json:"counter"`
}

// Ledger 是线程安全的内存复式账本，由上层负责持久化快照。
type Ledger struct {
	mu       sync.Mutex
	accounts map[string]*Account
	entries  []*Entry
	byTx     map[string][]*Entry
	counter  int64
}

func New() *Ledger {
	return &Ledger{
		accounts: make(map[string]*Account),
		byTx:     make(map[string][]*Entry),
	}
}

func (l *Ledger) EnsureAccount(id, name string, t AccountType, currency string) *Account {
	l.mu.Lock()
	defer l.mu.Unlock()
	if a, ok := l.accounts[id]; ok {
		return a
	}
	a := &Account{ID: id, Name: name, Type: t, Currency: currency}
	l.accounts[id] = a
	return a
}

var (
	ErrUnbalanced      = errors.New("ledger: postings not balanced")
	ErrAccountMissing  = errors.New("ledger: account missing")
	ErrNegativeAmount  = errors.New("ledger: negative amount")
	ErrInsufficient    = errors.New("ledger: insufficient credit balance")
	ErrTxAlreadyPosted = errors.New("ledger: transaction already posted")
)

// Post 以事务方式入账：借贷不平、透支等任何一步失败都不会留下半成品分录。
func (l *Ledger) Post(txID string, postings []Posting, remark string) ([]*Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.byTx[txID]; exists {
		return nil, fmt.Errorf("%w: %s", ErrTxAlreadyPosted, txID)
	}
	var debit, credit int64
	for _, p := range postings {
		if p.Amount < 0 {
			return nil, ErrNegativeAmount
		}
		if p.Amount == 0 {
			continue
		}
		if p.Debit {
			debit += p.Amount
		} else {
			credit += p.Amount
		}
	}
	if debit != credit {
		return nil, fmt.Errorf("%w: debit=%d credit=%d", ErrUnbalanced, debit, credit)
	}
	if debit == 0 {
		return nil, errors.New("ledger: empty posting")
	}

	// 透支校验：负债/收入类账户账户不能出现借方余额（如重复结算商户）。
	projected := make(map[string]int64)
	for _, p := range postings {
		if p.Amount == 0 {
			continue
		}
		a, ok := l.accounts[p.AccountID]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrAccountMissing, p.AccountID)
		}
		signed := p.Amount
		if !p.Debit {
			signed = -p.Amount
		}
		projected[p.AccountID] += signed
		if a.Type != Asset {
			bal := l.lockedBalance(p.AccountID) + projected[p.AccountID]
			if bal > 0 {
				return nil, fmt.Errorf("%w: account=%s", ErrInsufficient, p.AccountID)
			}
		}
	}

	now := time.Now().UnixNano()
	made := make([]*Entry, 0, len(postings))
	for _, p := range postings {
		if p.Amount == 0 {
			continue
		}
		l.counter++
		e := &Entry{
			ID:        fmt.Sprintf("E%06d", l.counter),
			TxID:      txID,
			AccountID: p.AccountID,
			Remark:    remark,
			CreatedAt: now,
		}
		if p.Debit {
			e.Debit = p.Amount
		} else {
			e.Credit = p.Amount
		}
		l.entries = append(l.entries, e)
		l.byTx[txID] = append(l.byTx[txID], e)
		made = append(made, e)
	}
	return made, nil
}

func (l *Ledger) lockedBalance(id string) int64 {
	var sum int64
	for _, e := range l.entries {
		if e.AccountID == id {
			sum += e.Debit - e.Credit
		}
	}
	return sum
}

// Balance 返回账户的“借-贷”带符号余额。
func (l *Ledger) Balance(accountID string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lockedBalance(accountID)
}

func (l *Ledger) Account(id string) *Account {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.accounts[id]
}

// Totals 返回全账本借贷合计，用于守恒校验，两者必须始终相等。
func (l *Ledger) Totals() (debit, credit int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		debit += e.Debit
		credit += e.Credit
	}
	return
}

func (l *Ledger) EntriesByTx(txID string) []*Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	src := l.byTx[txID]
	out := make([]*Entry, len(src))
	copy(out, src)
	return out
}

func (l *Ledger) Load(s *Snapshot) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if s == nil {
		l.accounts = make(map[string]*Account)
		l.entries = nil
		l.byTx = make(map[string][]*Entry)
		l.counter = 0
		return
	}
	// 与已注册的账户合并，避免上层先建账户再 Load 空快照时账户被清空。
	for _, a := range s.Accounts {
		l.accounts[a.ID] = a
	}
	l.entries = s.Entries
	l.byTx = make(map[string][]*Entry, len(s.Entries))
	for _, e := range s.Entries {
		l.byTx[e.TxID] = append(l.byTx[e.TxID], e)
	}
	l.counter = s.Counter
}

func (l *Ledger) Snapshot() *Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := &Snapshot{Counter: l.counter, Entries: l.entries}
	for _, a := range l.accounts {
		s.Accounts = append(s.Accounts, a)
	}
	return s
}
