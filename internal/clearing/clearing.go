// Package clearing 实现按结算周期的批量清算与结算。
//
// 规则：
//   - 批次由结算周期（period，如 2026-10-02）唯一确定，重复/并发触发
//     同一周期只会产生一份有效批次；
//   - 清算把账本中 ACCEPTED 状态的交易按商户归集，计算应收净额；
//   - 结算按商户逐个执行，单商户失败不影响其他商户，失败记录为
//     FAILED 可单独重试，已 SUCCESS 的商户重跑时跳过不重复结算；
//   - 全部状态持久化到本地 JSON，进程重启后已完成的不回退，
//     未完成的可继续推进。
package clearing

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
)

// 批次状态
const (
	BatchOpen      = "OPEN"      // 已创建，归集中
	BatchClearing  = "CLEARING"  // 清算完成，结算中
	BatchCompleted = "COMPLETED" // 全部商户结算成功
	BatchPartial   = "PARTIAL"   // 部分商户结算失败
)

// 商户结算状态
const (
	SettlePending = "PENDING"
	SettleSuccess = "SUCCESS"
	SettleFailed  = "FAILED"
)

var ErrBatchFinalized = errors.New("clearing: batch already completed")

// Settlement 单个商户在一个批次内的结算结果。
type Settlement struct {
	MerchantID string    `json:"merchantId"`
	NetAmount  int64     `json:"netAmount"` // 应收净额（最小单位）
	TxnIDs     []string  `json:"txnIds"`
	Status     string    `json:"status"` // PENDING / SUCCESS / FAILED
	FailReason string    `json:"failReason,omitempty"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Batch 一个结算周期对应一份批次。
type Batch struct {
	ID          string                 `json:"id"`
	Period      string                 `json:"period"`
	Status      string                 `json:"status"`
	Settlements map[string]*Settlement `json:"settlements"` // key: merchantID
	CreatedAt   time.Time              `json:"createdAt"`
	UpdatedAt   time.Time              `json:"updatedAt"`
}

type engineState struct {
	Batches  map[string]*Batch `json:"batches"`  // key: batchID
	ByPeriod map[string]string `json:"byPeriod"` // period -> batchID
}

// SettleHook 结算执行钩子，返回 error 表示该商户结算失败。
// 生产环境可对接出款通道；本地默认实现直接写账本。
type SettleHook func(batchID, merchantID string, amount int64, txnIDs []string) error

// Engine 清算结算引擎。
type Engine struct {
	mu    sync.Mutex
	lg    *ledger.Ledger
	store *ledger.Store
	state *engineState
	hook  SettleHook
}

// Open 打开（或创建）清算引擎，statePath 持久化批次状态。
// hook 为 nil 时使用默认账本记账。
func Open(statePath string, lg *ledger.Ledger, hook SettleHook) (*Engine, error) {
	store := ledger.NewStore(statePath)
	st := &engineState{Batches: make(map[string]*Batch), ByPeriod: make(map[string]string)}
	if err := store.Load(st); err != nil {
		return nil, err
	}
	if st.Batches == nil {
		st.Batches = make(map[string]*Batch)
	}
	if st.ByPeriod == nil {
		st.ByPeriod = make(map[string]string)
	}
	if hook == nil {
		hook = lg.SettleEntries
	}
	return &Engine{lg: lg, store: store, state: st, hook: hook}, nil
}

func (e *Engine) persistLocked() error { return e.store.Save(e.state) }

// RunBatch 触发一个周期的清算结算。
//
// 幂等：同一 period 重复/并发触发返回同一份批次；
// 已 SUCCESS 的商户不会重复结算，FAILED/PENDING 的商户继续推进。
func (e *Engine) RunBatch(period string) (*Batch, error) {
	e.mu.Lock()
	batch, err := e.getOrCreateBatchLocked(period)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if batch.Status == BatchOpen {
		if err := e.aggregateLocked(batch); err != nil {
			e.mu.Unlock()
			return nil, err
		}
	}
	e.mu.Unlock()

	// 结算在锁外逐商户执行，单商户失败不阻塞其他商户。
	e.settleBatch(batch.ID)

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state.Batches[batch.ID], nil
}

func (e *Engine) getOrCreateBatchLocked(period string) (*Batch, error) {
	if id, ok := e.state.ByPeriod[period]; ok {
		return e.state.Batches[id], nil
	}
	now := time.Now().UTC()
	batch := &Batch{
		ID:          "batch-" + period,
		Period:      period,
		Status:      BatchOpen,
		Settlements: make(map[string]*Settlement),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	e.state.Batches[batch.ID] = batch
	e.state.ByPeriod[period] = batch.ID
	if err := e.persistLocked(); err != nil {
		return nil, err
	}
	return batch, nil
}

// aggregateLocked 归集待清算交易，计算各商户净额并生成 PENDING 结算单。
func (e *Engine) aggregateLocked(batch *Batch) error {
	pending := e.lg.PendingTxns()
	byMerchant := make(map[string][]string)
	for _, txn := range pending {
		byMerchant[txn.MerchantID] = append(byMerchant[txn.MerchantID], txn.ID)
	}
	var txnIDs []string
	for merchantID, ids := range byMerchant {
		if _, ok := batch.Settlements[merchantID]; ok {
			continue // 已有该商户结算单（重跑场景），跳过
		}
		var net int64
		for _, id := range ids {
			txn, err := e.lg.GetTransaction(id)
			if err != nil {
				return err
			}
			net += txn.Amount
		}
		batch.Settlements[merchantID] = &Settlement{
			MerchantID: merchantID, NetAmount: net, TxnIDs: ids,
			Status: SettlePending, UpdatedAt: time.Now().UTC(),
		}
		txnIDs = append(txnIDs, ids...)
	}
	if err := e.lg.MarkCleared(txnIDs, batch.ID); err != nil {
		return err
	}
	batch.Status = BatchClearing
	batch.UpdatedAt = time.Now().UTC()
	return e.persistLocked()
}

// settleBatch 对批次内未成功的商户逐个执行结算。
func (e *Engine) settleBatch(batchID string) {
	e.mu.Lock()
	batch := e.state.Batches[batchID]
	var targets []*Settlement
	for _, s := range batch.Settlements {
		if s.Status != SettleSuccess {
			targets = append(targets, s)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].MerchantID < targets[j].MerchantID })
	e.mu.Unlock()

	for _, s := range targets {
		err := e.hook(batchID, s.MerchantID, s.NetAmount, s.TxnIDs)
		e.mu.Lock()
		b := e.state.Batches[batchID]
		cur := b.Settlements[s.MerchantID]
		if err != nil {
			cur.Status = SettleFailed
			cur.FailReason = err.Error()
		} else {
			cur.Status = SettleSuccess
			cur.FailReason = ""
		}
		cur.UpdatedAt = time.Now().UTC()
		b.UpdatedAt = cur.UpdatedAt
		_ = e.persistLocked()
		e.mu.Unlock()
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	b := e.state.Batches[batchID]
	allOK := true
	for _, s := range b.Settlements {
		if s.Status != SettleSuccess {
			allOK = false
			break
		}
	}
	if allOK {
		b.Status = BatchCompleted
	} else {
		b.Status = BatchPartial
	}
	b.UpdatedAt = time.Now().UTC()
	_ = e.persistLocked()
}

// RetryMerchant 单独重试批次内一个失败的商户。
func (e *Engine) RetryMerchant(batchID, merchantID string) (*Settlement, error) {
	e.mu.Lock()
	batch, ok := e.state.Batches[batchID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("clearing: batch %s not found", batchID)
	}
	s, ok := batch.Settlements[merchantID]
	if !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("clearing: merchant %s not in batch %s", merchantID, batchID)
	}
	if s.Status == SettleSuccess {
		out := *s
		e.mu.Unlock()
		return &out, nil // 幂等：已成功直接返回
	}
	e.mu.Unlock()

	err := e.hook(batchID, merchantID, s.NetAmount, s.TxnIDs)

	e.mu.Lock()
	defer e.mu.Unlock()
	b := e.state.Batches[batchID]
	cur := b.Settlements[merchantID]
	if err != nil {
		cur.Status = SettleFailed
		cur.FailReason = err.Error()
	} else {
		cur.Status = SettleSuccess
		cur.FailReason = ""
	}
	cur.UpdatedAt = time.Now().UTC()
	allOK := true
	for _, x := range b.Settlements {
		if x.Status != SettleSuccess {
			allOK = false
			break
		}
	}
	if allOK {
		b.Status = BatchCompleted
	} else {
		b.Status = BatchPartial
	}
	b.UpdatedAt = cur.UpdatedAt
	if perr := e.persistLocked(); perr != nil {
		return nil, perr
	}
	out := *cur
	return &out, err
}

// GetBatch 查询批次。
func (e *Engine) GetBatch(id string) (*Batch, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.state.Batches[id]
	return b, ok
}

// Batches 返回全部批次快照。
func (e *Engine) Batches() []*Batch {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*Batch, 0, len(e.state.Batches))
	for _, b := range e.state.Batches {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
