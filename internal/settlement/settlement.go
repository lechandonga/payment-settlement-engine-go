package settlement

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
)

const (
	AccountBankOut = "BANK_SETTLE_OUT" // 结算出款资金（资产）

	MerchantSuccess = "SUCCESS"
	MerchantFailed  = "FAILED"
	MerchantPending = "PENDING"
)

type MerchantResult struct {
	MerchantID  string   `json:"merchant_id"`
	PaymentIDs  []string `json:"payment_ids"`
	GrossAmount int64    `json:"gross_amount"`
	FeeAmount   int64    `json:"fee_amount"`
	NetAmount   int64    `json:"net_amount"`
	Status      string   `json:"status"`
	FailReason  string   `json:"fail_reason,omitempty"`
	Attempts    int      `json:"attempts"`
	EntryTxID   string   `json:"entry_tx_id,omitempty"`
	SettledAtNs int64    `json:"settled_at_ns,omitempty"`
}

type Batch struct {
	ID        string                     `json:"id"`
	Period    string                     `json:"period"`
	CreatedAt int64                      `json:"created_at_ns"`
	Merchants map[string]*MerchantResult `json:"merchants"`
}

func (b *Batch) AllDone() bool {
	for _, m := range b.Merchants {
		if m.Status != MerchantSuccess {
			return false
		}
	}
	return true
}

type Snapshot struct {
	Batches []*Batch `json:"batches"`
	Counter int64    `json:"counter"`
}

var (
	ErrBatchNotFound  = errors.New("settlement: batch not found")
	ErrMerchantNotIn  = errors.New("settlement: merchant not in batch")
	ErrNothingPending = errors.New("settlement: no pending payment for period")
)

type Service struct {
	lg  *ledger.Ledger
	pay *payment.Service

	mu       sync.Mutex
	batches  []*Batch
	byPeriod map[string]*Batch
	counter  int64

	// FailHook 用来模拟出款失败（如银行通道异常）。
	// 返回 true 时该商户本次清算标记 FAILED，不写结算分录；之后 Retry 可成功。
	FailHook func(merchantID string) error
}

func NewService(lg *ledger.Ledger, pay *payment.Service) *Service {
	lg.EnsureAccount(AccountBankOut, "结算出款资金", ledger.Asset, "CNY")
	return &Service{lg: lg, pay: pay, byPeriod: make(map[string]*Batch)}
}

// RunPeriod 触发某结算周期（如 "2026-10-01"）的清算。
// 并发触发同一周期只会产生一份有效批次：后到调用拿到同一份结果。
func (s *Service) RunPeriod(period string) (*Batch, error) {
	s.mu.Lock()
	if b, ok := s.byPeriod[period]; ok {
		s.mu.Unlock()
		return b, nil
	}

	pending := s.pay.PendingSettlement()
	groups := make(map[string][]*payment.Payment)
	for _, p := range pending {
		groups[p.MerchantID] = append(groups[p.MerchantID], p)
	}
	if len(groups) == 0 {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: period=%s", ErrNothingPending, period)
	}

	s.counter++
	batch := &Batch{
		ID:        fmt.Sprintf("S%s-%03d", period, s.counter),
		Period:    period,
		CreatedAt: time.Now().UnixNano(),
		Merchants: make(map[string]*MerchantResult),
	}

	merchantIDs := make([]string, 0, len(groups))
	for m := range groups {
		merchantIDs = append(merchantIDs, m)
	}
	sort.Strings(merchantIDs)
	for _, m := range merchantIDs {
		batch.Merchants[m] = &MerchantResult{MerchantID: m, Status: MerchantPending}
	}

	// 先落内存索引再处理：并发调用立即收敛到同一批次。
	s.batches = append(s.batches, batch)
	s.byPeriod[period] = batch
	s.mu.Unlock()

	for _, m := range merchantIDs {
		s.settleMerchant(batch, m, groups[m])
	}
	return batch, nil
}

// settleMerchant 对单个商户做清算。幂等：SUCCESS 的商户直接跳过，绝不重复出款。
func (s *Service) settleMerchant(batch *Batch, merchantID string, pays []*payment.Payment) *MerchantResult {
	s.mu.Lock()
	mr := batch.Merchants[merchantID]
	if mr.Status == MerchantSuccess {
		s.mu.Unlock()
		return mr
	}

	// 可能是重试路径：重新取该商户尚未结算的交易。
	if len(pays) == 0 {
		for _, p := range s.pay.PendingSettlement() {
			if p.MerchantID == merchantID {
				pays = append(pays, p)
			}
		}
	}
	if len(pays) == 0 {
		s.mu.Unlock()
		return mr
	}

	mr.Attempts++
	attempt := mr.Attempts
	var gross, fee int64
	ids := make([]string, 0, len(pays))
	for _, p := range pays {
		gross += p.Amount
		fee += p.Fee
		ids = append(ids, p.ID)
	}
	net := gross - fee

	hook := s.FailHook
	s.mu.Unlock()

	if hook != nil {
		if err := hook(merchantID); err != nil {
			s.mu.Lock()
			mr.Status = MerchantFailed
			mr.FailReason = err.Error()
			mr.Attempts = attempt
			s.mu.Unlock()
			return mr
		}
	}

	txID := fmt.Sprintf("TX:%s:%s", batch.ID, merchantID)
	_, err := s.lg.Post(txID, []ledger.Posting{
		{AccountID: payment.MerchantPayableAccount(merchantID), Amount: net, Debit: true},
		{AccountID: AccountBankOut, Amount: net, Debit: false},
	}, fmt.Sprintf("商户清算 %s 批次 %s", merchantID, batch.ID))
	if err != nil {
		s.mu.Lock()
		mr.Status = MerchantFailed
		mr.FailReason = err.Error()
		mr.Attempts = attempt
		s.mu.Unlock()
		return mr
	}

	s.pay.MarkSettled(ids, batch.Period)

	s.mu.Lock()
	mr.PaymentIDs = ids
	mr.GrossAmount = gross
	mr.FeeAmount = fee
	mr.NetAmount = net
	mr.Status = MerchantSuccess
	mr.FailReason = ""
	mr.Attempts = attempt
	mr.EntryTxID = txID
	mr.SettledAtNs = time.Now().UnixNano()
	s.mu.Unlock()
	return mr
}

// RetryMerchant 单独重试批次内失败/未完成的商户；已成功者不会被重复结算。
func (s *Service) RetryMerchant(batchID, merchantID string) (*MerchantResult, error) {
	s.mu.Lock()
	var batch *Batch
	for _, b := range s.batches {
		if b.ID == batchID {
			batch = b
			break
		}
	}
	if batch == nil {
		return nil, fmt.Errorf("%w: %s", ErrBatchNotFound, batchID)
	}
	mr, ok := batch.Merchants[merchantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrMerchantNotIn, batchID, merchantID)
	}
	if mr.Status == MerchantSuccess {
		s.mu.Unlock()
		return mr, nil
	}
	s.mu.Unlock()
	return s.settleMerchant(batch, merchantID, nil), nil
}

func (s *Service) GetBatch(id string) *Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.batches {
		if b.ID == id {
			return b
		}
	}
	return nil
}

func (s *Service) BatchByPeriod(period string) *Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byPeriod[period]
}

func (s *Service) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &Snapshot{Batches: s.batches, Counter: s.counter}
}

func (s *Service) Load(snap *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap == nil {
		s.batches = nil
		s.byPeriod = make(map[string]*Batch)
		s.counter = 0
		return
	}
	s.batches = snap.Batches
	s.counter = snap.Counter
	s.byPeriod = make(map[string]*Batch, len(snap.Batches))
	for _, b := range snap.Batches {
		s.byPeriod[b.Period] = b
	}
}
