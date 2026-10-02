package payment

import (
	"errors"
	"fmt"
	"sync"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
)

const (
	AccountReceivable = "ACQ_RECEIVABLE" // 待清算收单款（资产）
	AccountFeeIncome  = "FEES_INCOME"    // 手续费收入（收入）

	StatusAccepted = "ACCEPTED"
	StatusFailed   = "FAILED"
)

func MerchantPayableAccount(merchantID string) string {
	return "M_PAYABLE:" + merchantID
}

type Payment struct {
	ID           string `json:"id"`
	MerchantID   string `json:"merchant_id"`
	RequestNo    string `json:"request_no"`
	Amount       int64  `json:"amount"`
	Fee          int64  `json:"fee"`
	Status       string `json:"status"`
	EntryTxID    string `json:"entry_tx_id"`
	FailReason   string `json:"fail_reason,omitempty"`
	CreatedAtNs  int64  `json:"created_at_ns"`
	Settled      bool   `json:"settled"`
	SettlePeriod string `json:"settle_period,omitempty"`
}

func (p *Payment) NetAmount() int64 { return p.Amount - p.Fee }

type Request struct {
	MerchantID string
	RequestNo  string // 外部请求号，商户维度内唯一、幂等键
	Amount     int64  // 最小货币单位
}

// Result 是受理返回。Duplicate=true 表示本次为重复提交，Pay 为首次结果的精确回放。
type Result struct {
	Pay       *Payment
	Duplicate bool
}

type Snapshot struct {
	Payments []*Payment `json:"payments"`
	Counter  int64      `json:"counter"`
}

var (
	ErrInvalidRequest = errors.New("payment: invalid request")
	ErrRequestReplay  = errors.New("payment: request number replayed with different payload")
)

// Service 负责交易受理与幂等控制。所有状态随 Snapshot 持久化。
type Service struct {
	lg *ledger.Ledger

	mu       sync.Mutex
	payments []*Payment
	byReq    map[string]*Payment // merchantID + "|" + requestNo
	inflight sync.Map            // 并发重放时把同键请求串行化到首次处理
	counter  int64

	// FeeFn 计算手续费（最小单位，整数向下取整），默认 0.6%，测试可替换。
	FeeFn func(amount int64) int64
}

func NewService(lg *ledger.Ledger) *Service {
	lg.EnsureAccount(AccountReceivable, "待清算收单款", ledger.Asset, "CNY")
	lg.EnsureAccount(AccountFeeIncome, "手续费收入", ledger.Income, "CNY")
	s := &Service{lg: lg, byReq: make(map[string]*Payment)}
	s.FeeFn = func(amount int64) int64 { return amount * 6 / 1000 }
	return s
}

func key(merchantID, requestNo string) string { return merchantID + "|" + requestNo }

// Accept 受理一笔支付。同一外部请求号：
//   - 串行化（并发重放时后到者等首次完成）；
//   - 首次成功/失败结果原样返回，不重复扣款、不重复入账；
//   - 同请求号但金额/商户不同，明确拒绝（防止键复用）。
func (s *Service) Accept(req Request) (*Result, error) {
	if req.MerchantID == "" || req.RequestNo == "" || req.Amount <= 0 {
		return nil, fmt.Errorf("%w: merchant/requestNo required, amount must be positive", ErrInvalidRequest)
	}
	k := key(req.MerchantID, req.RequestNo)

	// fast path：已完成的首次结果
	s.mu.Lock()
	if first, ok := s.byReq[k]; ok {
		s.mu.Unlock()
		if first.Amount != req.Amount {
			return nil, fmt.Errorf("%w: key=%s", ErrRequestReplay, k)
		}
		return &Result{Pay: first, Duplicate: true}, nil
	}
	s.mu.Unlock()

	lockI, _ := s.inflight.LoadOrStore(k, &sync.Mutex{})
	lock := lockI.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	// 拿到锁后可能首次已经由别的 goroutine 完成
	s.mu.Lock()
	if first, ok := s.byReq[k]; ok {
		s.mu.Unlock()
		if first.Amount != req.Amount {
			return nil, fmt.Errorf("%w: key=%s", ErrRequestReplay, k)
		}
		return &Result{Pay: first, Duplicate: true}, nil
	}

	s.counter++
	pay := &Payment{
		ID:          fmt.Sprintf("P%06d", s.counter),
		MerchantID:  req.MerchantID,
		RequestNo:   req.RequestNo,
		Amount:      req.Amount,
		Fee:         s.FeeFn(req.Amount),
		Status:      StatusAccepted,
		CreatedAtNs: nowNs(),
	}

	merchantAcct := MerchantPayableAccount(req.MerchantID)
	s.lg.EnsureAccount(merchantAcct, "商户应付款-"+req.MerchantID, ledger.Liability, "CNY")

	txID := "TX:" + pay.ID
	_, err := s.lg.Post(txID, []ledger.Posting{
		{AccountID: AccountReceivable, Amount: pay.Amount, Debit: true},
		{AccountID: merchantAcct, Amount: pay.NetAmount(), Debit: false},
		{AccountID: AccountFeeIncome, Amount: pay.Fee, Debit: false},
	}, "受理支付 "+pay.ID)
	if err != nil {
		s.counter--
		s.mu.Unlock()
		return nil, fmt.Errorf("payment: post ledger: %w", err)
	}
	pay.EntryTxID = txID

	s.payments = append(s.payments, pay)
	s.byReq[k] = pay
	s.mu.Unlock()
	return &Result{Pay: pay, Duplicate: false}, nil
}

// PendingSettlement 返回某周期内尚未清算的已受理交易。
func (s *Service) PendingSettlement() []*Payment {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Payment, 0)
	for _, p := range s.payments {
		if p.Status == StatusAccepted && !p.Settled {
			out = append(out, p)
		}
	}
	return out
}

// MarkSettled 由清算服务在商户清算成功后调用。
func (s *Service) MarkSettled(paymentIDs []string, period string) {
	set := make(map[string]bool, len(paymentIDs))
	for _, id := range paymentIDs {
		set[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.payments {
		if set[p.ID] {
			p.Settled = true
			p.SettlePeriod = period
		}
	}
}

func (s *Service) Get(id string) *Payment {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.payments {
		if p.ID == id {
			return p
		}
	}
	return nil
}

func (s *Service) List() []*Payment {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Payment, len(s.payments))
	copy(out, s.payments)
	return out
}

func (s *Service) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &Snapshot{Payments: s.payments, Counter: s.counter}
}

func (s *Service) Load(snap *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap == nil {
		s.payments = nil
		s.byReq = make(map[string]*Payment)
		s.counter = 0
		return
	}
	s.payments = snap.Payments
	s.counter = snap.Counter
	s.byReq = make(map[string]*Payment, len(snap.Payments))
	for _, p := range snap.Payments {
		s.byReq[key(p.MerchantID, p.RequestNo)] = p
	}
}
