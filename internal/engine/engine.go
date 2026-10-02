package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
	"github.com/lechandonga/payment-settlement-engine-go/internal/reconcile"
	"github.com/lechandonga/payment-settlement-engine-go/internal/settlement"
)

type Engine struct {
	Ledger     *ledger.Ledger
	Payments   *payment.Service
	Settlement *settlement.Service
	Reconcile  *reconcile.Service

	dir string
}

// New 创建引擎。dir 为本地状态目录，已存在文件时自动恢复（进程重启场景）。
func New(dir string) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: mkdir: %w", err)
	}
	e := &Engine{
		Ledger:   ledger.New(),
		Payments: nil,
		dir:      dir,
	}
	e.Payments = payment.NewService(e.Ledger)
	e.Settlement = settlement.NewService(e.Ledger, e.Payments)
	e.Reconcile = reconcile.NewService(e.Ledger, e.Payments, e.Settlement)

	if err := e.load(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) path(name string) string { return filepath.Join(e.dir, name) }

// Save 将全部状态原子落盘（写临时文件后 rename）。
// 每完成一个关键步骤调用一次，崩溃只会落在一个步骤之前，不会出现半个状态。
func (e *Engine) Save() error {
	if err := writeJSON(e.path("ledger.json"), e.Ledger.Snapshot()); err != nil {
		return err
	}
	if err := writeJSON(e.path("payments.json"), e.Payments.Snapshot()); err != nil {
		return err
	}
	if err := writeJSON(e.path("settlement.json"), e.Settlement.Snapshot()); err != nil {
		return err
	}
	return nil
}

func (e *Engine) load() error {
	var ls ledger.Snapshot
	if err := readJSONIfExists(e.path("ledger.json"), &ls); err != nil {
		return err
	}
	e.Ledger.Load(&ls)

	// 重建服务（确保账户都注册），再灌入快照
	var ps payment.Snapshot
	if err := readJSONIfExists(e.path("payments.json"), &ps); err != nil {
		return err
	}
	e.Payments.Load(&ps)

	var ss settlement.Snapshot
	if err := readJSONIfExists(e.path("settlement.json"), &ss); err != nil {
		return err
	}
	e.Settlement.Load(&ss)
	return nil
}

// CheckConsistency 做两条守恒校验：
//  1. 全账本借方合计 == 贷方合计；
//  2. 账户余额 == 该账户分录借-贷汇总（结构本身决定恒成立，作为自检）。
func (e *Engine) CheckConsistency() error {
	debit, credit := e.Ledger.Totals()
	if debit != credit {
		return fmt.Errorf("engine: ledger not balanced: debit=%d credit=%d", debit, credit)
	}

	sumByAcct := make(map[string]int64)
	snap := e.Ledger.Snapshot()
	for _, en := range snap.Entries {
		sumByAcct[en.AccountID] += en.Debit - en.Credit
	}
	for id, want := range sumByAcct {
		if got := e.Ledger.Balance(id); got != want {
			return fmt.Errorf("engine: balance mismatch account=%s balance=%d entries=%d", id, got, want)
		}
	}

	// 已结算交易金额必须与批次商户结果一致，且清算分录金额等于商户净额。
	for _, b := range e.Settlement.Snapshot().Batches {
		for _, m := range b.Merchants {
			if m.Status == settlement.MerchantSuccess {
				var gross, fee int64
				for _, id := range m.PaymentIDs {
					p := e.Payments.Get(id)
					if p == nil || !p.Settled {
						return fmt.Errorf("engine: settled payment missing %s", id)
					}
					gross += p.Amount
					fee += p.Fee
				}
				if gross != m.GrossAmount || fee != m.FeeAmount || gross-fee != m.NetAmount {
					return fmt.Errorf("engine: batch totals mismatch %s/%s", b.ID, m.MerchantID)
				}
				if e.Ledger.Balance(payment.MerchantPayableAccount(m.MerchantID)) > 0 {
					return fmt.Errorf("engine: merchant payable over-settled %s", m.MerchantID)
				}
			}
		}
	}
	return nil
}

func writeJSON(path string, v any) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("engine: create %s: %w", tmp, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSONIfExists(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(v); err != nil {
		return fmt.Errorf("engine: decode %s: %w", path, err)
	}
	return nil
}

func (e *Engine) Dir() string { return e.dir }
