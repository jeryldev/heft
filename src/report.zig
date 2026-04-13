const impl = @import("report_impl.zig");

pub const MAX_REPORT_ROWS = impl.MAX_REPORT_ROWS;
pub const ReportRow = impl.ReportRow;
pub const ReportResult = impl.ReportResult;
pub const TransactionRow = impl.TransactionRow;
pub const LedgerResult = impl.LedgerResult;
pub const ComparativeReportRow = impl.ComparativeReportRow;
pub const ComparativeReportResult = impl.ComparativeReportResult;
pub const EquityRow = impl.EquityRow;
pub const EquityResult = impl.EquityResult;
pub const TranslationRates = impl.TranslationRates;

pub const generalLedger = impl.generalLedger;
pub const accountLedger = impl.accountLedger;
pub const journalRegister = impl.journalRegister;
pub const trialBalance = impl.trialBalance;
pub const incomeStatement = impl.incomeStatement;
pub const trialBalanceMovement = impl.trialBalanceMovement;
pub const balanceSheetAuto = impl.balanceSheetAuto;
pub const balanceSheet = impl.balanceSheet;
pub const balanceSheetAutoWithProjectedRE = impl.balanceSheetAutoWithProjectedRE;
pub const balanceSheetWithProjectedRE = impl.balanceSheetWithProjectedRE;
pub const trialBalanceComparative = impl.trialBalanceComparative;
pub const incomeStatementComparative = impl.incomeStatementComparative;
pub const balanceSheetComparative = impl.balanceSheetComparative;
pub const trialBalanceMovementComparative = impl.trialBalanceMovementComparative;
pub const equityChanges = impl.equityChanges;
pub const translateReportResult = impl.translateReportResult;

comptime {
    _ = impl;
}
