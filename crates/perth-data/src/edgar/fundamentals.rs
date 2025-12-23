//! SEC EDGAR fundamental data extraction.
//!
//! This module provides types and functions for extracting financial statement data
//! from SEC EDGAR filings using the XBRL JSON API.

use crate::error::{DataError, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Quarterly or annual financial data from SEC filings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinancialStatement {
    /// Stock symbol
    pub symbol: String,
    /// Central Index Key (CIK) - SEC identifier
    pub cik: String,
    /// Period end date
    pub period_end: NaiveDate,
    /// Period type (Quarterly or Annual)
    pub period_type: PeriodType,
    /// Fiscal year
    pub fiscal_year: i32,
    /// Fiscal quarter (1-4 for quarterly filings, None for annual)
    pub fiscal_quarter: Option<i32>,

    // Balance Sheet Items
    /// Total assets
    pub total_assets: Option<f64>,
    /// Total liabilities
    pub total_liabilities: Option<f64>,
    /// Stockholders' equity (also known as shareholders' equity)
    pub stockholders_equity: Option<f64>,
    /// Long-term debt
    pub long_term_debt: Option<f64>,
    /// Current assets
    pub current_assets: Option<f64>,
    /// Current liabilities
    pub current_liabilities: Option<f64>,
    /// Cash and cash equivalents
    pub cash_and_equivalents: Option<f64>,

    // Income Statement Items
    /// Total revenue (also known as net sales)
    pub revenue: Option<f64>,
    /// Net income (also known as net earnings or profit)
    pub net_income: Option<f64>,
    /// Operating income
    pub operating_income: Option<f64>,
    /// Gross profit
    pub gross_profit: Option<f64>,
    /// Basic earnings per share
    pub eps_basic: Option<f64>,
    /// Diluted earnings per share
    pub eps_diluted: Option<f64>,

    // Cash Flow Items
    /// Operating cash flow
    pub operating_cash_flow: Option<f64>,
    /// Capital expenditures
    pub capital_expenditures: Option<f64>,
    /// Free cash flow (Operating CF - CapEx)
    pub free_cash_flow: Option<f64>,

    // Share Information
    /// Common shares outstanding (basic)
    pub shares_outstanding: Option<f64>,
    /// Common shares outstanding (diluted)
    pub shares_outstanding_diluted: Option<f64>,
}

/// Period type for financial statements.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PeriodType {
    /// Quarterly (10-Q) filing
    Quarterly,
    /// Annual (10-K) filing
    Annual,
}

impl PeriodType {
    /// Convert form type to period type.
    pub fn from_form(form: &str) -> Option<Self> {
        match form {
            "10-Q" => Some(Self::Quarterly),
            "10-K" => Some(Self::Annual),
            _ => None,
        }
    }
}

/// Pre-computed inputs ready for factor calculations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactorInputs {
    /// Book value per share
    pub book_value_per_share: Option<f64>,
    /// Earnings per share (diluted)
    pub earnings_per_share: Option<f64>,
    /// Return on equity (Net Income / Avg Equity)
    pub roe: Option<f64>,
    /// Return on assets (Net Income / Avg Assets)
    pub roa: Option<f64>,
    /// Debt to equity ratio
    pub debt_to_equity: Option<f64>,
    /// Current ratio (Current Assets / Current Liabilities)
    pub current_ratio: Option<f64>,
    /// Revenue growth year-over-year (requires prior period)
    pub revenue_growth_yoy: Option<f64>,
    /// Earnings growth year-over-year (requires prior period)
    pub earnings_growth_yoy: Option<f64>,
    /// Price to book ratio
    pub price_to_book: Option<f64>,
    /// Price to earnings ratio
    pub price_to_earnings: Option<f64>,
}

/// Response from the SEC EDGAR Company Facts API.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CompanyFactsResponse {
    /// CIK number
    pub cik: u64,
    /// Entity name
    #[serde(rename = "entityName")]
    pub entity_name: String,
    /// Facts organized by taxonomy and tag
    pub facts: HashMap<String, HashMap<String, TagFacts>>,
}

/// Facts for a specific XBRL tag.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TagFacts {
    /// Label/description
    pub label: String,
    /// Description
    pub description: Option<String>,
    /// Units (USD, shares, etc.) containing the actual fact values
    pub units: Option<HashMap<String, Vec<FactValue>>>,
}

/// A single fact value with metadata.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct FactValue {
    /// End date of the period
    pub end: String,
    /// Value
    pub val: f64,
    /// Accession number
    #[serde(default)]
    pub accn: Option<String>,
    /// Fiscal year
    #[serde(default)]
    pub fy: Option<i32>,
    /// Fiscal period
    #[serde(default)]
    pub fp: Option<String>,
    /// Form type
    #[serde(default)]
    pub form: Option<String>,
    /// Filed date
    #[serde(default)]
    pub filed: Option<String>,
    /// Frame (instant or duration)
    #[serde(default)]
    pub frame: Option<String>,
}

/// Maps common financial concepts to their possible XBRL tags.
///
/// Different companies and even the same company across different periods
/// may use different XBRL tags for the same concept. This mapping handles
/// the most common variations.
#[derive(Debug)]
struct XbrlTagMapper {
    /// Map of concept name to list of possible XBRL tags
    tags: HashMap<String, Vec<String>>,
}

impl XbrlTagMapper {
    /// Create a new XBRL tag mapper with standard mappings.
    pub(crate) fn new() -> Self {
        let mut tags: HashMap<String, Vec<String>> = HashMap::new();

        // Assets
        tags.insert("Assets".to_string(), vec!["Assets".to_string()]);

        tags.insert(
            "AssetsCurrent".to_string(),
            vec!["AssetsCurrent".to_string()],
        );

        // Liabilities
        tags.insert(
            "Liabilities".to_string(),
            vec![
                "Liabilities".to_string(),
                "LiabilitiesAndStockholdersEquity".to_string(),
            ],
        );

        tags.insert(
            "LiabilitiesCurrent".to_string(),
            vec!["LiabilitiesCurrent".to_string()],
        );

        tags.insert(
            "LongTermDebt".to_string(),
            vec![
                "LongTermDebt".to_string(),
                "LongTermDebtNoncurrent".to_string(),
                "LongTermDebtAndCapitalLeaseObligations".to_string(),
            ],
        );

        // Equity
        tags.insert(
            "StockholdersEquity".to_string(),
            vec![
                "StockholdersEquity".to_string(),
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
                    .to_string(),
            ],
        );

        // Cash
        tags.insert(
            "CashAndCashEquivalents".to_string(),
            vec![
                "CashAndCashEquivalentsAtCarryingValue".to_string(),
                "Cash".to_string(),
                "CashCashEquivalentsAndShortTermInvestments".to_string(),
            ],
        );

        // Revenue
        tags.insert(
            "Revenue".to_string(),
            vec![
                "Revenues".to_string(),
                "RevenueFromContractWithCustomerExcludingAssessedTax".to_string(),
                "SalesRevenueNet".to_string(),
                "RevenueFromContractWithCustomerIncludingAssessedTax".to_string(),
            ],
        );

        // Net Income
        tags.insert(
            "NetIncome".to_string(),
            vec![
                "NetIncomeLoss".to_string(),
                "ProfitLoss".to_string(),
                "NetIncomeLossAvailableToCommonStockholdersBasic".to_string(),
            ],
        );

        // Operating Income
        tags.insert("OperatingIncome".to_string(), vec![
            "OperatingIncomeLoss".to_string(),
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest".to_string(),
        ]);

        // Gross Profit
        tags.insert("GrossProfit".to_string(), vec!["GrossProfit".to_string()]);

        // EPS
        tags.insert(
            "EarningsPerShareBasic".to_string(),
            vec!["EarningsPerShareBasic".to_string()],
        );

        tags.insert(
            "EarningsPerShareDiluted".to_string(),
            vec!["EarningsPerShareDiluted".to_string()],
        );

        // Cash Flow
        tags.insert(
            "OperatingCashFlow".to_string(),
            vec![
                "NetCashProvidedByUsedInOperatingActivities".to_string(),
                "CashProvidedByUsedInOperatingActivities".to_string(),
            ],
        );

        tags.insert(
            "CapitalExpenditures".to_string(),
            vec![
                "PaymentsToAcquirePropertyPlantAndEquipment".to_string(),
                "PaymentsForCapitalImprovements".to_string(),
            ],
        );

        // Shares
        tags.insert(
            "SharesOutstanding".to_string(),
            vec![
                "CommonStockSharesOutstanding".to_string(),
                "CommonStockSharesIssued".to_string(),
            ],
        );

        tags.insert(
            "WeightedAverageNumberOfSharesOutstandingBasic".to_string(),
            vec!["WeightedAverageNumberOfSharesOutstandingBasic".to_string()],
        );

        tags.insert(
            "WeightedAverageNumberOfDilutedSharesOutstanding".to_string(),
            vec!["WeightedAverageNumberOfDilutedSharesOutstanding".to_string()],
        );

        Self { tags }
    }

    /// Get possible XBRL tags for a concept.
    pub(crate) fn get_tags(&self, concept: &str) -> Option<&Vec<String>> {
        self.tags.get(concept)
    }
}

/// Provider for SEC EDGAR fundamental data.
#[derive(Debug)]
pub struct EdgarFundamentalsProvider {
    /// HTTP client
    client: reqwest::Client,
    /// XBRL tag mapper
    tag_mapper: XbrlTagMapper,
}

impl EdgarFundamentalsProvider {
    /// Create a new EDGAR fundamentals provider.
    ///
    /// The SEC requires a User-Agent header with contact information.
    /// This implementation uses a generic user agent. In production,
    /// you should replace this with your company name and email.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent("Perth Factor Model (perth@factordynamics.io)")
                .build()
                .expect("Failed to create HTTP client"),
            tag_mapper: XbrlTagMapper::new(),
        }
    }

    /// Fetch company CIK from symbol.
    ///
    /// The SEC uses CIK (Central Index Key) to identify companies.
    /// This method looks up the CIK for a given ticker symbol.
    async fn fetch_cik(&self, symbol: &str) -> Result<String> {
        // The SEC provides a company tickers JSON file that maps symbols to CIKs
        let url = "https://www.sec.gov/files/company_tickers.json";

        let response = self.client.get(url).send().await?;

        if !response.status().is_success() {
            return Err(DataError::Http(format!(
                "Failed to fetch company tickers: {}",
                response.status()
            )));
        }

        let tickers: HashMap<String, serde_json::Value> = response.json().await?;

        // Search for the symbol in the tickers
        for (_, company) in tickers.iter() {
            if let Some(ticker) = company.get("ticker").and_then(|v| v.as_str())
                && ticker.eq_ignore_ascii_case(symbol)
                && let Some(cik) = company.get("cik_str")
            {
                // CIK can be a number or string
                let cik_str = match cik {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => continue,
                };
                // Pad CIK to 10 digits
                return Ok(format!("{:0>10}", cik_str));
            }
        }

        Err(DataError::CikNotFound(symbol.to_string()))
    }

    /// Fetch company facts from SEC EDGAR.
    ///
    /// This uses the SEC's Company Facts API which returns all XBRL facts
    /// for a company in a single JSON response.
    async fn fetch_company_facts(&self, cik: &str) -> Result<CompanyFactsResponse> {
        let url = format!("https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json", cik);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(DataError::EdgarApi(format!(
                "Failed to fetch company facts for CIK {}: {}",
                cik,
                response.status()
            )));
        }

        let facts: CompanyFactsResponse = response.json().await?;
        Ok(facts)
    }

    /// Extract a fact value from company facts response.
    ///
    /// Tries multiple XBRL tag names and returns the most recent value
    /// matching the specified period type and fiscal period.
    fn extract_fact(
        &self,
        facts: &CompanyFactsResponse,
        concept: &str,
        period_type: Option<PeriodType>,
        fiscal_year: Option<i32>,
        fiscal_period: Option<&str>,
    ) -> Option<f64> {
        let tags = self.tag_mapper.get_tags(concept)?;

        // Try US-GAAP taxonomy first, then DEI (Document and Entity Information)
        for taxonomy in ["us-gaap", "dei"] {
            if let Some(taxonomy_facts) = facts.facts.get(taxonomy) {
                for tag in tags {
                    if let Some(tag_facts) = taxonomy_facts.get(tag)
                        && let Some(units) = &tag_facts.units
                    {
                        // Try USD first for monetary values, then shares, then pure numbers
                        for unit_type in ["USD", "shares", "pure"] {
                            if let Some(values) = units.get(unit_type) {
                                // Filter by period type and fiscal period if specified
                                let filtered: Vec<&FactValue> = values
                                    .iter()
                                    .filter(|v| {
                                        // Filter by form type if period type is specified
                                        if let Some(pt) = period_type
                                            && let Some(form) = &v.form
                                        {
                                            match pt {
                                                PeriodType::Quarterly => {
                                                    if form != "10-Q" {
                                                        return false;
                                                    }
                                                }
                                                PeriodType::Annual => {
                                                    if form != "10-K" {
                                                        return false;
                                                    }
                                                }
                                            }
                                        }

                                        // Filter by fiscal year if specified
                                        if let Some(fy) = fiscal_year
                                            && v.fy != Some(fy)
                                        {
                                            return false;
                                        }

                                        // Filter by fiscal period if specified
                                        if let Some(fp) = fiscal_period
                                            && let Some(v_fp) = &v.fp
                                            && v_fp != fp
                                        {
                                            return false;
                                        }

                                        true
                                    })
                                    .collect();

                                // Return the most recent value
                                if let Some(fact) = filtered.last() {
                                    return Some(fact.val);
                                }
                            }
                        }
                    }
                }
            }
        }

        None
    }

    /// Fetch all available financial statements for a company.
    ///
    /// This method fetches the company facts and extracts financial statements
    /// for all available periods.
    pub async fn fetch_financials(&self, symbol: &str) -> Result<Vec<FinancialStatement>> {
        let cik = self.fetch_cik(symbol).await?;
        let facts = self.fetch_company_facts(&cik).await?;

        let mut statements = Vec::new();

        // Extract unique periods from the facts
        let mut periods: HashMap<(i32, String, String), (NaiveDate, String)> = HashMap::new();

        // Scan through all facts to find unique periods
        for taxonomy_facts in facts.facts.values() {
            for tag_facts in taxonomy_facts.values() {
                if let Some(units) = &tag_facts.units {
                    for values in units.values() {
                        for value in values {
                            if let (Some(fy), Some(fp), Some(form)) =
                                (&value.fy, &value.fp, &value.form)
                                && (form == "10-Q" || form == "10-K")
                            {
                                // Parse end date
                                if let Ok(end_date) =
                                    NaiveDate::parse_from_str(&value.end, "%Y-%m-%d")
                                {
                                    periods.insert(
                                        (*fy, fp.clone(), form.clone()),
                                        (end_date, form.clone()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Extract financial statement for each period
        for ((fy, fp, form), (end_date, _)) in periods {
            let period_type = PeriodType::from_form(&form).unwrap_or(PeriodType::Quarterly);
            let fiscal_quarter = if period_type == PeriodType::Quarterly {
                // Extract quarter number from fiscal period (Q1, Q2, Q3, Q4)
                fp.chars()
                    .nth(1)
                    .and_then(|c| c.to_digit(10))
                    .map(|d| d as i32)
            } else {
                None
            };

            let stmt = self.extract_statement(
                &facts,
                symbol,
                &cik,
                end_date,
                period_type,
                fy,
                fiscal_quarter,
                Some(&fp),
            );

            statements.push(stmt);
        }

        // Sort by period end date (most recent first)
        statements.sort_by(|a, b| b.period_end.cmp(&a.period_end));

        Ok(statements)
    }

    /// Extract a single financial statement for a specific period.
    #[allow(clippy::too_many_arguments)]
    fn extract_statement(
        &self,
        facts: &CompanyFactsResponse,
        symbol: &str,
        cik: &str,
        period_end: NaiveDate,
        period_type: PeriodType,
        fiscal_year: i32,
        fiscal_quarter: Option<i32>,
        fiscal_period: Option<&str>,
    ) -> FinancialStatement {
        // Extract all financial metrics
        let total_assets = self.extract_fact(
            facts,
            "Assets",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let current_assets = self.extract_fact(
            facts,
            "AssetsCurrent",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let total_liabilities = self.extract_fact(
            facts,
            "Liabilities",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let current_liabilities = self.extract_fact(
            facts,
            "LiabilitiesCurrent",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let stockholders_equity = self.extract_fact(
            facts,
            "StockholdersEquity",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let long_term_debt = self.extract_fact(
            facts,
            "LongTermDebt",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let cash_and_equivalents = self.extract_fact(
            facts,
            "CashAndCashEquivalents",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );

        let revenue = self.extract_fact(
            facts,
            "Revenue",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let net_income = self.extract_fact(
            facts,
            "NetIncome",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let operating_income = self.extract_fact(
            facts,
            "OperatingIncome",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let gross_profit = self.extract_fact(
            facts,
            "GrossProfit",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let eps_basic = self.extract_fact(
            facts,
            "EarningsPerShareBasic",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let eps_diluted = self.extract_fact(
            facts,
            "EarningsPerShareDiluted",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );

        let operating_cash_flow = self.extract_fact(
            facts,
            "OperatingCashFlow",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );
        let capital_expenditures = self.extract_fact(
            facts,
            "CapitalExpenditures",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );

        // Calculate free cash flow if both components are available
        let free_cash_flow = match (operating_cash_flow, capital_expenditures) {
            (Some(ocf), Some(capex)) => Some(ocf - capex),
            _ => None,
        };

        let shares_outstanding = self
            .extract_fact(
                facts,
                "SharesOutstanding",
                Some(period_type),
                Some(fiscal_year),
                fiscal_period,
            )
            .or_else(|| {
                self.extract_fact(
                    facts,
                    "WeightedAverageNumberOfSharesOutstandingBasic",
                    Some(period_type),
                    Some(fiscal_year),
                    fiscal_period,
                )
            });
        let shares_outstanding_diluted = self.extract_fact(
            facts,
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            Some(period_type),
            Some(fiscal_year),
            fiscal_period,
        );

        FinancialStatement {
            symbol: symbol.to_string(),
            cik: cik.to_string(),
            period_end,
            period_type,
            fiscal_year,
            fiscal_quarter,
            total_assets,
            total_liabilities,
            stockholders_equity,
            long_term_debt,
            current_assets,
            current_liabilities,
            cash_and_equivalents,
            revenue,
            net_income,
            operating_income,
            gross_profit,
            eps_basic,
            eps_diluted,
            operating_cash_flow,
            capital_expenditures,
            free_cash_flow,
            shares_outstanding,
            shares_outstanding_diluted,
        }
    }

    /// Get the most recent quarterly statement.
    pub async fn fetch_latest_quarterly(&self, symbol: &str) -> Result<FinancialStatement> {
        let statements = self.fetch_financials(symbol).await?;

        statements
            .into_iter()
            .find(|s| s.period_type == PeriodType::Quarterly)
            .ok_or_else(|| DataError::MissingData {
                symbol: symbol.to_string(),
                reason: "No quarterly statements found".to_string(),
            })
    }

    /// Get the most recent annual statement.
    pub async fn fetch_latest_annual(&self, symbol: &str) -> Result<FinancialStatement> {
        let statements = self.fetch_financials(symbol).await?;

        statements
            .into_iter()
            .find(|s| s.period_type == PeriodType::Annual)
            .ok_or_else(|| DataError::MissingData {
                symbol: symbol.to_string(),
                reason: "No annual statements found".to_string(),
            })
    }

    /// Compute derived metrics for factor calculations.
    ///
    /// This method takes a financial statement and current market price
    /// and computes various financial ratios and metrics useful for
    /// factor-based investing strategies.
    pub fn compute_factor_inputs(&self, stmt: &FinancialStatement, price: f64) -> FactorInputs {
        // Book value per share
        let book_value_per_share = match (stmt.stockholders_equity, stmt.shares_outstanding) {
            (Some(equity), Some(shares)) if shares > 0.0 => Some(equity / shares),
            _ => None,
        };

        // Price to book ratio
        let price_to_book =
            book_value_per_share.map(|bvps| if bvps > 0.0 { price / bvps } else { f64::NAN });

        // Earnings per share (use diluted if available, otherwise basic)
        let earnings_per_share = stmt.eps_diluted.or(stmt.eps_basic);

        // Price to earnings ratio
        let price_to_earnings =
            earnings_per_share.map(|eps| if eps > 0.0 { price / eps } else { f64::NAN });

        // Return on equity (ROE)
        let roe = match (stmt.net_income, stmt.stockholders_equity) {
            (Some(ni), Some(eq)) if eq > 0.0 => Some(ni / eq),
            _ => None,
        };

        // Return on assets (ROA)
        let roa = match (stmt.net_income, stmt.total_assets) {
            (Some(ni), Some(assets)) if assets > 0.0 => Some(ni / assets),
            _ => None,
        };

        // Debt to equity ratio
        let debt_to_equity = match (stmt.long_term_debt, stmt.stockholders_equity) {
            (Some(debt), Some(equity)) if equity > 0.0 => Some(debt / equity),
            _ => None,
        };

        // Current ratio
        let current_ratio = match (stmt.current_assets, stmt.current_liabilities) {
            (Some(assets), Some(liabilities)) if liabilities > 0.0 => Some(assets / liabilities),
            _ => None,
        };

        FactorInputs {
            book_value_per_share,
            earnings_per_share,
            roe,
            roa,
            debt_to_equity,
            current_ratio,
            revenue_growth_yoy: None,  // Requires prior period comparison
            earnings_growth_yoy: None, // Requires prior period comparison
            price_to_book,
            price_to_earnings,
        }
    }

    /// Compute factor inputs with year-over-year growth metrics.
    ///
    /// This method takes the current and prior year statements to compute
    /// growth metrics.
    pub fn compute_factor_inputs_with_growth(
        &self,
        current: &FinancialStatement,
        prior: &FinancialStatement,
        price: f64,
    ) -> FactorInputs {
        let mut inputs = self.compute_factor_inputs(current, price);

        // Revenue growth YoY
        inputs.revenue_growth_yoy = match (current.revenue, prior.revenue) {
            (Some(curr_rev), Some(prior_rev)) if prior_rev > 0.0 => {
                Some((curr_rev - prior_rev) / prior_rev)
            }
            _ => None,
        };

        // Earnings growth YoY
        inputs.earnings_growth_yoy = match (current.net_income, prior.net_income) {
            (Some(curr_ni), Some(prior_ni)) if prior_ni > 0.0 => {
                Some((curr_ni - prior_ni) / prior_ni)
            }
            _ => None,
        };

        inputs
    }
}

impl Default for EdgarFundamentalsProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_period_type_from_form() {
        assert_eq!(PeriodType::from_form("10-Q"), Some(PeriodType::Quarterly));
        assert_eq!(PeriodType::from_form("10-K"), Some(PeriodType::Annual));
        assert_eq!(PeriodType::from_form("8-K"), None);
    }

    #[test]
    fn test_xbrl_tag_mapper() {
        let mapper = XbrlTagMapper::new();

        assert!(mapper.get_tags("Assets").is_some());
        assert!(mapper.get_tags("Revenue").is_some());
        assert!(mapper.get_tags("NetIncome").is_some());
        assert!(mapper.get_tags("NonexistentConcept").is_none());
    }

    #[test]
    fn test_compute_factor_inputs() {
        let provider = EdgarFundamentalsProvider::new();

        let stmt = FinancialStatement {
            symbol: "TEST".to_string(),
            cik: "0000000001".to_string(),
            period_end: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            period_type: PeriodType::Annual,
            fiscal_year: 2024,
            fiscal_quarter: None,
            total_assets: Some(1_000_000.0),
            total_liabilities: Some(400_000.0),
            stockholders_equity: Some(600_000.0),
            long_term_debt: Some(200_000.0),
            current_assets: Some(300_000.0),
            current_liabilities: Some(100_000.0),
            cash_and_equivalents: Some(50_000.0),
            revenue: Some(500_000.0),
            net_income: Some(50_000.0),
            operating_income: Some(75_000.0),
            gross_profit: Some(200_000.0),
            eps_basic: Some(5.0),
            eps_diluted: Some(4.8),
            operating_cash_flow: Some(60_000.0),
            capital_expenditures: Some(20_000.0),
            free_cash_flow: Some(40_000.0),
            shares_outstanding: Some(10_000.0),
            shares_outstanding_diluted: Some(10_416.0),
        };

        let price = 100.0;
        let inputs = provider.compute_factor_inputs(&stmt, price);

        // Book value per share = 600,000 / 10,000 = 60.0
        assert_eq!(inputs.book_value_per_share, Some(60.0));

        // EPS (diluted)
        assert_eq!(inputs.earnings_per_share, Some(4.8));

        // ROE = 50,000 / 600,000 = 0.0833...
        assert!(inputs.roe.is_some());
        assert!((inputs.roe.unwrap() - 0.0833).abs() < 0.001);

        // ROA = 50,000 / 1,000,000 = 0.05
        assert_eq!(inputs.roa, Some(0.05));

        // Debt to equity = 200,000 / 600,000 = 0.3333...
        assert!(inputs.debt_to_equity.is_some());
        assert!((inputs.debt_to_equity.unwrap() - 0.3333).abs() < 0.001);

        // Current ratio = 300,000 / 100,000 = 3.0
        assert_eq!(inputs.current_ratio, Some(3.0));

        // P/B = 100 / 60 = 1.6666...
        assert!(inputs.price_to_book.is_some());
        assert!((inputs.price_to_book.unwrap() - 1.6667).abs() < 0.001);

        // P/E = 100 / 4.8 = 20.833...
        assert!(inputs.price_to_earnings.is_some());
        assert!((inputs.price_to_earnings.unwrap() - 20.833).abs() < 0.01);
    }

    #[test]
    fn test_compute_factor_inputs_with_growth() {
        let provider = EdgarFundamentalsProvider::new();

        let current = FinancialStatement {
            symbol: "TEST".to_string(),
            cik: "0000000001".to_string(),
            period_end: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            period_type: PeriodType::Annual,
            fiscal_year: 2024,
            fiscal_quarter: None,
            total_assets: Some(1_000_000.0),
            total_liabilities: Some(400_000.0),
            stockholders_equity: Some(600_000.0),
            long_term_debt: Some(200_000.0),
            current_assets: Some(300_000.0),
            current_liabilities: Some(100_000.0),
            cash_and_equivalents: Some(50_000.0),
            revenue: Some(500_000.0),
            net_income: Some(50_000.0),
            operating_income: Some(75_000.0),
            gross_profit: Some(200_000.0),
            eps_basic: Some(5.0),
            eps_diluted: Some(4.8),
            operating_cash_flow: Some(60_000.0),
            capital_expenditures: Some(20_000.0),
            free_cash_flow: Some(40_000.0),
            shares_outstanding: Some(10_000.0),
            shares_outstanding_diluted: Some(10_416.0),
        };

        let prior = FinancialStatement {
            symbol: "TEST".to_string(),
            cik: "0000000001".to_string(),
            period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            period_type: PeriodType::Annual,
            fiscal_year: 2023,
            fiscal_quarter: None,
            total_assets: Some(900_000.0),
            total_liabilities: Some(380_000.0),
            stockholders_equity: Some(520_000.0),
            long_term_debt: Some(180_000.0),
            current_assets: Some(280_000.0),
            current_liabilities: Some(95_000.0),
            cash_and_equivalents: Some(45_000.0),
            revenue: Some(400_000.0),
            net_income: Some(40_000.0),
            operating_income: Some(65_000.0),
            gross_profit: Some(180_000.0),
            eps_basic: Some(4.0),
            eps_diluted: Some(3.8),
            operating_cash_flow: Some(50_000.0),
            capital_expenditures: Some(18_000.0),
            free_cash_flow: Some(32_000.0),
            shares_outstanding: Some(10_000.0),
            shares_outstanding_diluted: Some(10_526.0),
        };

        let price = 100.0;
        let inputs = provider.compute_factor_inputs_with_growth(&current, &prior, price);

        // Revenue growth = (500,000 - 400,000) / 400,000 = 0.25 (25%)
        assert_eq!(inputs.revenue_growth_yoy, Some(0.25));

        // Earnings growth = (50,000 - 40,000) / 40,000 = 0.25 (25%)
        assert_eq!(inputs.earnings_growth_yoy, Some(0.25));
    }
}
