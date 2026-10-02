package reconcile

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/lechandonga/payment-settlement-engine-go/internal/ledger"
	"github.com/lechandonga/payment-settlement-engine-go/internal/payment"
	"github.com/lechandonga/payment-settlement-engine-go/internal/settlement"
)

const (
	AccountAdjust = "RECON_ADJUST" // 对账差异挂账（资产/待查）

	TypeMatched          = "MATCHED"           // 逐笔一致
	TypeMissingLedger    = "MISSING_LEDGER"    // 结算单有、账本无（银行多付/漏记）
	TypeMissingStatement = "MISSING_STATEMENT" // 账本有、结算单无（银行少付/漏单）
	TypeAmountMismatch   = "AMOUNT_MISMATCH"   // 金额不符
	TypeDuplicateInStmt  = "DUPLICATE_STMT"    // 结算单重复记录
)

// StmtRow 是本地结算单的一行：merchant_id,payment_id,net_amount
type StmtRow struct {
	MerchantID string
	PaymentID  string
	Amount     int64
	LineNo     int
}

type Record struct {
	Type            string `json:"type"`
	MerchantID      string `json:"merchant_id"`
	PaymentID       string `json:"payment_id"`
	LedgerAmount    int64  `json:"ledger_amount"`
	StatementAmount int64  `json:"statement_amount"`
	DiffAmount      int64  `json:"diff_amount"`
	LineNo          int    `json:"line_no,omitempty"`
	AdjustTxID      string `json:"adjust_tx_id,omitempty"`
	AdjustRemark    string `json:"adjust_remark,omitempty"`
	Note            string `json:"note,omitempty"`
}

type Report struct {
	GeneratedAtNs int64          `json:"generated_at_ns"`
	Records       []*Record      `json:"records"`
	Summary       map[string]int `json:"summary"`
}

// ParseStatement 读取本地 CSV 结算单。表头：merchant_id,payment_id,net_amount
func ParseStatement(r io.Reader) ([]*StmtRow, error) {
	cr := csv.NewReader(bufio.NewReader(r))
	cr.FieldsPerRecord = -1
	rows := make([]*StmtRow, 0)
	line := 0
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reconcile: csv line %d: %w", line+1, err)
		}
		line++
		if len(rec) < 3 {
			return nil, fmt.Errorf("reconcile: csv line %d: need 3 columns", line)
		}
		mid, pid := strings.TrimSpace(rec[0]), strings.TrimSpace(rec[1])
		if line == 1 && strings.EqualFold(pid, "payment_id") {
			continue // 表头
		}
		amt, err := strconv.ParseInt(strings.TrimSpace(rec[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("reconcile: csv line %d: bad amount: %w", line, err)
		}
		rows = append(rows, &StmtRow{MerchantID: mid, PaymentID: pid, Amount: amt, LineNo: line})
	}
	return rows, nil
}

type Service struct {
	lg     *ledger.Ledger
	pay    *payment.Service
	settle *settlement.Service
}

func NewService(lg *ledger.Ledger, pay *payment.Service, settle *settlement.Service) *Service {
	lg.EnsureAccount(AccountAdjust, "对账差异待查", ledger.Asset, "CNY")
	return &Service{lg: lg, pay: pay, settle: settle}
}

// Compare 逐笔比对但不落调整分录，先看全量差异分类。
func (s *Service) Compare(rows []*StmtRow) *Report {
	report := &Report{GeneratedAtNs: time.Now().UnixNano(), Summary: map[string]int{}}

	seen := make(map[string][]*StmtRow)
	for _, r := range rows {
		seen[r.PaymentID] = append(seen[r.PaymentID], r)
	}

	matchedStmt := make(map[int]bool)
	ledgerPays := s.pay.List()
	ledgerByID := make(map[string]*payment.Payment, len(ledgerPays))
	for _, p := range ledgerPays {
		ledgerByID[p.ID] = p
	}

	for pid, rs := range seen {
		primary := rs[0]
		for _, dup := range rs[1:] {
			matchedStmt[dup.LineNo] = true
			report.Records = append(report.Records, &Record{
				Type: TypeDuplicateInStmt, MerchantID: dup.MerchantID, PaymentID: pid,
				StatementAmount: dup.Amount, LineNo: dup.LineNo,
				Note: "结算单中重复出现，以首条为准，不重复调账",
			})
		}
		p, ok := ledgerByID[pid]
		if !ok {
			report.Records = append(report.Records, &Record{
				Type: TypeMissingLedger, MerchantID: primary.MerchantID, PaymentID: pid,
				StatementAmount: primary.Amount, LineNo: primary.LineNo,
			})
			matchedStmt[primary.LineNo] = true
			continue
		}
		net := p.NetAmount()
		if net != primary.Amount {
			diff := primary.Amount - net
			report.Records = append(report.Records, &Record{
				Type: TypeAmountMismatch, MerchantID: p.MerchantID, PaymentID: pid,
				LedgerAmount: net, StatementAmount: primary.Amount, DiffAmount: diff,
				LineNo: primary.LineNo,
			})
		} else {
			report.Records = append(report.Records, &Record{
				Type: TypeMatched, MerchantID: p.MerchantID, PaymentID: pid,
				LedgerAmount: net, StatementAmount: primary.Amount, LineNo: primary.LineNo,
			})
		}
		matchedStmt[primary.LineNo] = true
	}

	// 账本里已清算但结算单完全没有的交易
	stmtIDs := make(map[string]bool, len(rows))
	for _, r := range rows {
		stmtIDs[r.PaymentID] = true
	}
	for _, p := range ledgerPays {
		if p.Settled && !stmtIDs[p.ID] {
			report.Records = append(report.Records, &Record{
				Type: TypeMissingStatement, MerchantID: p.MerchantID, PaymentID: p.ID,
				LedgerAmount: p.NetAmount(), DiffAmount: -p.NetAmount(),
			})
		}
	}

	for _, rec := range report.Records {
		report.Summary[rec.Type]++
	}
	return report
}

// Adjust 根据报告对每条差异做可追溯调账。重复记录无需改账（账本本就只有一笔）。
// 每笔调整都是借贷相等的一组分录，调账前后全账本借贷总额必然守恒。
func (s *Service) Adjust(report *Report) error {
	if report == nil {
		return errors.New("reconcile: nil report")
	}
	for _, rec := range report.Records {
		if rec.AdjustTxID != "" || rec.Type == TypeMatched || rec.Type == TypeDuplicateInStmt {
			continue
		}
		txID := fmt.Sprintf("TX:ADJ:%s:%d", rec.PaymentID, rec.LineNo)
		remark := fmt.Sprintf("对账调账 %s 原交易=%s", rec.Type, rec.PaymentID)

		switch rec.Type {
		case TypeMissingLedger:
			// 银行按结算单付了款、账本缺结算分录：补记出款。
			p := s.pay.Get(rec.PaymentID)
			if p != nil {
				if _, err := s.lg.Post(txID, []ledger.Posting{
					{AccountID: payment.MerchantPayableAccount(p.MerchantID), Amount: rec.StatementAmount, Debit: true},
					{AccountID: settlement.AccountBankOut, Amount: rec.StatementAmount, Debit: false},
				}, remark); err != nil {
					return err
				}
				s.pay.MarkSettled([]string{p.ID}, "RECON")
			} else {
				// 原交易在账本中不存在：先挂待查，不直接核销商户。
				if _, err := s.lg.Post(txID, []ledger.Posting{
					{AccountID: AccountAdjust, Amount: rec.StatementAmount, Debit: true},
					{AccountID: settlement.AccountBankOut, Amount: rec.StatementAmount, Debit: false},
				}, remark+"（原交易缺失，挂待查）"); err != nil {
					return err
				}
			}
			rec.AdjustRemark = "补记结算出款"
		case TypeMissingStatement:
			// 账本已结算但银行实际未付：冲回结算分录。
			if _, err := s.lg.Post(txID, []ledger.Posting{
				{AccountID: settlement.AccountBankOut, Amount: rec.LedgerAmount, Debit: true},
				{AccountID: payment.MerchantPayableAccount(rec.MerchantID), Amount: rec.LedgerAmount, Debit: false},
			}, remark); err != nil {
				return err
			}
			rec.AdjustRemark = "冲回多记结算"
		case TypeAmountMismatch:
			diff := rec.DiffAmount // 结算单 - 账本
			if diff > 0 {
				// 银行多付：补出款，差额挂待查。
				if _, err := s.lg.Post(txID, []ledger.Posting{
					{AccountID: AccountAdjust, Amount: diff, Debit: true},
					{AccountID: settlement.AccountBankOut, Amount: diff, Debit: false},
				}, remark); err != nil {
					return err
				}
			} else if diff < 0 {
				d := -diff
				if _, err := s.lg.Post(txID, []ledger.Posting{
					{AccountID: settlement.AccountBankOut, Amount: d, Debit: true},
					{AccountID: AccountAdjust, Amount: d, Debit: false},
				}, remark); err != nil {
					return err
				}
			}
			rec.AdjustRemark = "差额挂待查科目"
		}
		rec.AdjustTxID = txID
	}
	return nil
}
