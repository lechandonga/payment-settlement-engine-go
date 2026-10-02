// Package reconcile 实现本地结算单与内部账本的逐笔对账。
//
// 差异分类（任何一条记录都必须落到一个分类，不允许静默忽略）：
//   - DiffMissingInStatement：账本有、结算单没有（单边缺失-缺外部）
//   - DiffMissingInLedger：结算单有、账本没有（单边缺失-缺内部）
//   - DiffAmountMismatch：两边都有但金额不符
//   - DiffDuplicate：同一交易号在任一侧出现多次
//
// 差异调整通过 PostAdjustment 写回账本，adjustID 关联差异与原始交易号，
// 调整分录借贷自动平衡，保证调整前后账本借贷总额守恒。
package reconcile

import (
	"fmt"
	"sort"
	"time"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
)

// 差异类型
const (
	DiffMissingInStatement = "MISSING_IN_STATEMENT"
	DiffMissingInLedger    = "MISSING_IN_LEDGER"
	DiffAmountMismatch     = "AMOUNT_MISMATCH"
	DiffDuplicate          = "DUPLICATE"
)

// StatementLine 结算单（外部文件）中的一行。
type StatementLine struct {
	LineID     string `json:"lineId"`
	ExternalID string `json:"externalId"` // 对应账本交易的 ExternalID
	Amount     int64  `json:"amount"`     // 最小货币单位
}

// Diff 一条对账差异。
type Diff struct {
	Type            string `json:"type"`
	ExternalID      string `json:"externalId"`
	LedgerAmount    int64  `json:"ledgerAmount,omitempty"`
	StatementAmount int64  `json:"statementAmount,omitempty"`
	Detail          string `json:"detail"`
}

// Report 对账报告：差异列表 + 汇总，总数必须等于两侧记录数之和的覆盖。
type Report struct {
	StatementLines int       `json:"statementLines"`
	LedgerTxns     int       `json:"ledgerTxns"`
	Matched        int       `json:"matched"`
	Diffs          []*Diff   `json:"diffs"`
	CreatedAt      time.Time `json:"createdAt"`
}

// Reconciler 对账器。
type Reconciler struct {
	lg *ledger.Ledger
}

func New(lg *ledger.Ledger) *Reconciler { return &Reconciler{lg: lg} }

// Reconcile 将结算单与账本中的支付交易（Kind=PAYMENT）逐笔比对，
// 结算、调整等内部记账记录不参与比对。
// 返回的报告覆盖两侧每一条记录：匹配、或落入某个差异分类。
func (r *Reconciler) Reconcile(lines []StatementLine) *Report {
	txns := r.lg.Transactions()

	payments := txns[:0:0]
	for _, t := range txns {
		if t.Kind == ledger.KindPayment || t.Kind == "" {
			payments = append(payments, t)
		}
	}
	txns = payments

	ledgerByExt := make(map[string][]*ledger.Transaction)
	for _, t := range txns {
		ledgerByExt[t.ExternalID] = append(ledgerByExt[t.ExternalID], t)
	}
	stmtByExt := make(map[string][]StatementLine)
	for _, l := range lines {
		stmtByExt[l.ExternalID] = append(stmtByExt[l.ExternalID], l)
	}

	report := &Report{
		StatementLines: len(lines),
		LedgerTxns:     len(txns),
		CreatedAt:      time.Now().UTC(),
	}

	extIDs := make(map[string]bool)
	for id := range ledgerByExt {
		extIDs[id] = true
	}
	for id := range stmtByExt {
		extIDs[id] = true
	}
	sorted := make([]string, 0, len(extIDs))
	for id := range extIDs {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)

	for _, extID := range sorted {
		ltxns := ledgerByExt[extID]
		slines := stmtByExt[extID]

		if len(ltxns) > 1 {
			report.Diffs = append(report.Diffs, &Diff{
				Type: DiffDuplicate, ExternalID: extID,
				Detail: fmt.Sprintf("ledger has %d records for %s", len(ltxns), extID),
			})
		}
		if len(slines) > 1 {
			report.Diffs = append(report.Diffs, &Diff{
				Type: DiffDuplicate, ExternalID: extID,
				Detail: fmt.Sprintf("statement has %d lines for %s", len(slines), extID),
			})
		}

		switch {
		case len(ltxns) > 0 && len(slines) == 0:
			report.Diffs = append(report.Diffs, &Diff{
				Type: DiffMissingInStatement, ExternalID: extID,
				LedgerAmount: ltxns[0].Amount,
				Detail:       "present in ledger, absent in statement",
			})
		case len(ltxns) == 0 && len(slines) > 0:
			report.Diffs = append(report.Diffs, &Diff{
				Type: DiffMissingInLedger, ExternalID: extID,
				StatementAmount: slines[0].Amount,
				Detail:          "present in statement, absent in ledger",
			})
		case len(ltxns) > 0 && len(slines) > 0:
			if ltxns[0].Amount != slines[0].Amount {
				report.Diffs = append(report.Diffs, &Diff{
					Type: DiffAmountMismatch, ExternalID: extID,
					LedgerAmount: ltxns[0].Amount, StatementAmount: slines[0].Amount,
					Detail: fmt.Sprintf("ledger=%d statement=%d", ltxns[0].Amount, slines[0].Amount),
				})
			} else {
				report.Matched++
			}
		}
	}
	return report
}

// Adjust 针对一条差异在账本中补记调整分录，可追溯原始交易。
//
// 调整策略：
//   - MISSING_IN_LEDGER：按结算单金额补记一笔应收（借 应收户 / 贷 调整户）；
//   - AMOUNT_MISMATCH：按差额补记（多退少补）；
//   - 其他类型返回错误，需人工处理。
//
// adjustID 幂等，重复调用不会重复入账；分录借贷平衡，账本保持守恒。
func (r *Reconciler) Adjust(adjustID string, diff *Diff, merchantID string) error {
	accountID := "acct:recv:" + merchantID
	switch diff.Type {
	case DiffMissingInLedger:
		return r.lg.PostAdjustment(adjustID, diff.ExternalID, accountID, diff.StatementAmount, 0,
			"reconcile: book missing txn "+diff.ExternalID)
	case DiffAmountMismatch:
		delta := diff.StatementAmount - diff.LedgerAmount
		if delta > 0 {
			return r.lg.PostAdjustment(adjustID, diff.ExternalID, accountID, delta, 0,
				"reconcile: top-up "+diff.ExternalID)
		}
		return r.lg.PostAdjustment(adjustID, diff.ExternalID, accountID, 0, -delta,
			"reconcile: write-down "+diff.ExternalID)
	default:
		return fmt.Errorf("reconcile: diff type %s requires manual handling", diff.Type)
	}
}
