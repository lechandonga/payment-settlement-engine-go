// Package ledger 实现基于借贷复式记账的本地账本。
//
// 模型：
//   - Account：账户，余额由分录汇总推导，不单独存储，保证任意时刻可对账。
//   - Transaction：一笔支付受理记录，以商户外部请求号（ExternalID）去重。
//   - Entry：借贷分录，每笔交易生成一借一贷两条分录，金额相等，
//     因此整个账本任意时刻借贷总额守恒。
//
// 金额一律使用最小货币单位（分）的 int64，避免浮点误差。
package ledger

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// 账户类型
const (
	AccountMerchantReceivable = "MERCHANT_RECEIVABLE" // 商户应收（资产）
	AccountMerchantPayable    = "MERCHANT_PAYABLE"    // 商户应付（负债）
	AccountClearing           = "CLEARING"            // 平台清算过渡户
	AccountAdjustment         = "ADJUSTMENT"          // 对账调整过渡户
)

// 交易状态
const (
	TxnStatusAccepted = "ACCEPTED" // 已受理记账，待清算
	TxnStatusCleared  = "CLEARED"  // 已纳入清算批次
	TxnStatusSettled  = "SETTLED"  // 已结算
)

var (
	ErrInvalidAmount = errors.New("ledger: amount must be positive")
	ErrDuplicate     = errors.New("ledger: duplicate external id with mismatched payload")
	ErrTxnNotFound   = errors.New("ledger: transaction not found")
)

// 交易类别
const (
	KindPayment    = "PAYMENT"    // 商户支付
	KindSettlement = "SETTLEMENT" // 清算结算内部记账
	KindAdjustment = "ADJUSTMENT" // 对账调整
)

// Account 账户。余额不持久化，由分录汇总计算。
type Account struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// Entry 一条借贷分录。Debit 与 Credit 有且仅有一个为正数。
type Entry struct {
	ID        string    `json:"id"`
	TxnID     string    `json:"txnId"`
	AccountID string    `json:"accountId"`
	Debit     int64     `json:"debit"`
	Credit    int64     `json:"credit"`
	Memo      string    `json:"memo"`
	CreatedAt time.Time `json:"createdAt"`
}

// Transaction 一笔支付交易及其分录。
type Transaction struct {
	ID         string    `json:"id"`
	ExternalID string    `json:"externalId"` // 商户外部请求号，幂等键
	Kind       string    `json:"kind"`       // PAYMENT / SETTLEMENT / ADJUSTMENT
	MerchantID string    `json:"merchantId"`
	Amount     int64     `json:"amount"` // 最小货币单位
	Status     string    `json:"status"`
	BatchID    string    `json:"batchId,omitempty"`
	Entries    []Entry   `json:"entries"`
	CreatedAt  time.Time `json:"createdAt"`
}

// PostResult 受理结果。重复提交返回与首次一致的结果，Duplicate 为 true。
type PostResult struct {
	Txn       *Transaction
	Duplicate bool
}

// Ledger 账本。所有写操作持锁并立即持久化，保证重启后可恢复。
type Ledger struct {
	mu        sync.Mutex
	store     *Store
	state     *ledgerState
	idCounter int
}

type ledgerState struct {
	Accounts map[string]*Account     `json:"accounts"`
	Txns     map[string]*Transaction `json:"txns"`    // key: txnID
	ByExtID  map[string]string       `json:"byExtId"` // externalID -> txnID
	Entries  []*Entry                `json:"entries"` // 全量分录流水
}

func newState() *ledgerState {
	return &ledgerState{
		Accounts: make(map[string]*Account),
		Txns:     make(map[string]*Transaction),
		ByExtID:  make(map[string]string),
	}
}

// Open 打开（或创建）位于 path 的账本，重启后状态从文件恢复。
func Open(path string) (*Ledger, error) {
	store := NewStore(path)
	st := newState()
	if err := store.Load(st); err != nil {
		return nil, err
	}
	l := &Ledger{store: store, state: st}
	for _, txn := range st.Txns {
		l.idCounter = maxID(l.idCounter, txn.ID)
		for i := range txn.Entries {
			l.idCounter = maxID(l.idCounter, txn.Entries[i].ID)
		}
	}
	return l, nil
}

func maxID(cur int, id string) int {
	var n int
	if _, err := fmt.Sscanf(id, "%d", &n); err == nil && n > cur {
		return n
	}
	return cur
}

func (l *Ledger) nextID() string {
	l.idCounter++
	return fmt.Sprintf("%d", l.idCounter)
}

func (l *Ledger) persistLocked() error {
	return l.store.Save(l.state)
}

// EnsureAccount 确保账户存在，返回账户 ID。
func (l *Ledger) EnsureAccount(id, name, typ string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.state.Accounts[id]; ok {
		return nil
	}
	l.state.Accounts[id] = &Account{ID: id, Name: name, Type: typ}
	return l.persistLocked()
}

// merchantAccountID 商户应收户账号。
func merchantAccountID(merchantID string) string { return "acct:recv:" + merchantID }

// PostPayment 受理一笔支付并记账：借 商户应收，贷 清算过渡户。
//
// 幂等：同一 externalID 重复提交（含并发重放）只入账一次；
// 若重复提交的金额或商户与首次不一致，返回 ErrDuplicate 明确拒绝。
func (l *Ledger) PostPayment(externalID, merchantID string, amount int64) (*PostResult, error) {
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if txnID, ok := l.state.ByExtID[externalID]; ok {
		txn := l.state.Txns[txnID]
		if txn.MerchantID != merchantID || txn.Amount != amount {
			return nil, fmt.Errorf("%w: %s", ErrDuplicate, externalID)
		}
		return &PostResult{Txn: txn, Duplicate: true}, nil
	}

	now := time.Now().UTC()
	txnID := "txn-" + l.nextID()
	recvAcct := merchantAccountID(merchantID)
	if _, ok := l.state.Accounts[recvAcct]; !ok {
		l.state.Accounts[recvAcct] = &Account{ID: recvAcct, Name: "merchant-" + merchantID, Type: AccountMerchantReceivable}
	}
	if _, ok := l.state.Accounts[AccountClearing]; !ok {
		l.state.Accounts[AccountClearing] = &Account{ID: AccountClearing, Name: "clearing", Type: AccountClearing}
	}
	entries := []Entry{
		{ID: "e" + l.nextID(), TxnID: txnID, AccountID: recvAcct, Debit: amount, Memo: "payment receivable", CreatedAt: now},
		{ID: "e" + l.nextID(), TxnID: txnID, AccountID: AccountClearing, Credit: amount, Memo: "payment clearing", CreatedAt: now},
	}
	txn := &Transaction{
		ID: txnID, ExternalID: externalID, MerchantID: merchantID,
		Amount: amount, Kind: KindPayment, Status: TxnStatusAccepted, Entries: entries, CreatedAt: now,
	}
	l.state.Txns[txnID] = txn
	l.state.ByExtID[externalID] = txnID
	for i := range entries {
		e := entries[i]
		l.state.Entries = append(l.state.Entries, &e)
	}
	if err := l.persistLocked(); err != nil {
		return nil, err
	}
	return &PostResult{Txn: txn}, nil
}

// Balance 账户余额 = 借方合计 - 贷方合计（最小单位）。
func (l *Ledger) Balance(accountID string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balanceLocked(accountID)
}

func (l *Ledger) balanceLocked(accountID string) int64 {
	var bal int64
	for _, e := range l.state.Entries {
		if e.AccountID == accountID {
			bal += e.Debit - e.Credit
		}
	}
	return bal
}

// Totals 返回全账本借方、贷方总额，二者必须相等（守恒校验）。
func (l *Ledger) Totals() (debit, credit int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.state.Entries {
		debit += e.Debit
		credit += e.Credit
	}
	return debit, credit
}

// GetTransaction 按 ID 查询交易。
func (l *Ledger) GetTransaction(id string) (*Transaction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	txn, ok := l.state.Txns[id]
	if !ok {
		return nil, ErrTxnNotFound
	}
	return txn, nil
}

// PendingTxns 返回待清算（ACCEPTED）交易，按 ID 排序保证确定性。
func (l *Ledger) PendingTxns() []*Transaction {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []*Transaction
	for _, t := range l.state.Txns {
		if t.Status == TxnStatusAccepted {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MarkCleared 将一批交易标记为已清算并归属批次。
func (l *Ledger) MarkCleared(txnIDs []string, batchID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, id := range txnIDs {
		txn, ok := l.state.Txns[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTxnNotFound, id)
		}
		if txn.Status == TxnStatusAccepted {
			txn.Status = TxnStatusCleared
			txn.BatchID = batchID
		}
	}
	return l.persistLocked()
}

// SettleEntries 为商户结算记账：借 清算过渡户，贷 商户应付户，
// 并将交易标记为已结算。重复调用同一批次同一商户不会产生重复分录。
func (l *Ledger) SettleEntries(batchID, merchantID string, amount int64, txnIDs []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	payable := "acct:pay:" + merchantID
	if _, ok := l.state.Accounts[payable]; !ok {
		l.state.Accounts[payable] = &Account{ID: payable, Name: "merchant-pay-" + merchantID, Type: AccountMerchantPayable}
	}
	now := time.Now().UTC()
	settleTxnID := "settle-" + batchID + "-" + merchantID
	if _, exists := l.state.Txns[settleTxnID]; !exists {
		entries := []Entry{
			{ID: "e" + l.nextID(), TxnID: settleTxnID, AccountID: AccountClearing, Debit: amount, Memo: "settle out", CreatedAt: now},
			{ID: "e" + l.nextID(), TxnID: settleTxnID, AccountID: payable, Credit: amount, Memo: "settle payable", CreatedAt: now},
		}
		l.state.Txns[settleTxnID] = &Transaction{
			ID: settleTxnID, ExternalID: settleTxnID, MerchantID: merchantID,
			Amount: amount, Kind: KindSettlement, Status: TxnStatusSettled, BatchID: batchID, Entries: entries, CreatedAt: now,
		}
		for i := range entries {
			e := entries[i]
			l.state.Entries = append(l.state.Entries, &e)
		}
	}
	for _, id := range txnIDs {
		if txn, ok := l.state.Txns[id]; ok {
			txn.Status = TxnStatusSettled
		}
	}
	return l.persistLocked()
}

// PostAdjustment 录入对账调整分录，借贷自动平衡，refTxnID 追溯原始交易。
// debit 与 credit 有且仅有一个为正数；同一 adjustID 幂等。
func (l *Ledger) PostAdjustment(adjustID, refTxnID, accountID string, debit, credit int64, memo string) error {
	if (debit > 0) == (credit > 0) {
		return errors.New("ledger: adjustment must be exactly one-sided")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.state.Accounts[AccountAdjustment]; !ok {
		l.state.Accounts[AccountAdjustment] = &Account{ID: AccountAdjustment, Name: "adjustment", Type: AccountAdjustment}
	}
	if _, ok := l.state.Txns[adjustID]; ok {
		return nil // 调整幂等
	}
	now := time.Now().UTC()
	var entries []Entry
	if debit > 0 {
		entries = []Entry{
			{ID: "e" + l.nextID(), TxnID: adjustID, AccountID: accountID, Debit: debit, Memo: memo, CreatedAt: now},
			{ID: "e" + l.nextID(), TxnID: adjustID, AccountID: AccountAdjustment, Credit: debit, Memo: memo, CreatedAt: now},
		}
	} else {
		entries = []Entry{
			{ID: "e" + l.nextID(), TxnID: adjustID, AccountID: AccountAdjustment, Debit: credit, Memo: memo, CreatedAt: now},
			{ID: "e" + l.nextID(), TxnID: adjustID, AccountID: accountID, Credit: credit, Memo: memo, CreatedAt: now},
		}
	}
	l.state.Txns[adjustID] = &Transaction{
		ID: adjustID, ExternalID: adjustID, MerchantID: "ADJ:" + refTxnID,
		Amount: debit + credit, Kind: KindAdjustment, Status: TxnStatusSettled, Entries: entries, CreatedAt: now,
	}
	for i := range entries {
		e := entries[i]
		l.state.Entries = append(l.state.Entries, &e)
	}
	return l.persistLocked()
}

// Entries 返回全量分录快照（用于对账导出）。
func (l *Ledger) Entries() []*Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Entry, len(l.state.Entries))
	copy(out, l.state.Entries)
	return out
}

// Transactions 返回全部交易快照。
func (l *Ledger) Transactions() []*Transaction {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Transaction, 0, len(l.state.Txns))
	for _, t := range l.state.Txns {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
