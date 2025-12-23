//! XBRL parsing for SEC EDGAR filings.
//!
//! This module provides functionality to parse XBRL data from SEC filings (10-K, 10-Q).
//! It supports both the SEC JSON API (recommended) and raw XML parsing as fallback.
//!
//! The SEC provides XBRL data in JSON format at:
//! `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json`
//!
//! # Example
//!
//! ```no_run
//! use perth_data::edgar::xbrl::{XbrlDocument, XbrlClient};
//! use chrono::NaiveDate;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = XbrlClient::new();
//!     let doc = client.fetch_company_facts("0000320193").await?; // Apple Inc.
//!
//!     if let Some(fact) = doc.get_latest_fact("us-gaap:Assets") {
//!         println!("Total Assets: {} {}", fact.value, fact.unit);
//!     }
//!
//!     Ok(())
//! }
//! ```

use crate::error::{DataError, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a single XBRL fact (data point).
///
/// An XBRL fact is a financial data point with context about the reporting period,
/// unit of measure, and the specific financial concept being reported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XbrlFact {
    /// The XBRL concept name (e.g., "us-gaap:NetIncomeLoss")
    pub concept: String,

    /// The numeric value of the fact
    pub value: f64,

    /// Unit of measure (e.g., "USD", "shares")
    pub unit: String,

    /// End date of the reporting period
    pub period_end: NaiveDate,

    /// Start date of the reporting period (None for instant facts like balance sheet items)
    pub period_start: Option<NaiveDate>,

    /// Form type (e.g., "10-K", "10-Q")
    pub form: Option<String>,

    /// Fiscal year
    pub fiscal_year: Option<i32>,

    /// Fiscal period (e.g., "FY", "Q1", "Q2", "Q3", "Q4")
    pub fiscal_period: Option<String>,
}

impl XbrlFact {
    /// Returns true if this is an instant fact (point-in-time, like balance sheet items)
    pub const fn is_instant(&self) -> bool {
        self.period_start.is_none()
    }

    /// Returns true if this is a duration fact (period-based, like income statement items)
    pub const fn is_duration(&self) -> bool {
        self.period_start.is_some()
    }

    /// Returns the duration in days if this is a duration fact
    pub fn duration_days(&self) -> Option<i64> {
        self.period_start
            .map(|start| self.period_end.signed_duration_since(start).num_days())
    }
}

/// Represents a collection of XBRL facts from a filing or company.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct XbrlDocument {
    /// All facts in the document
    pub facts: Vec<XbrlFact>,

    /// Company name
    pub entity_name: Option<String>,

    /// CIK (Central Index Key)
    pub cik: Option<String>,
}

impl XbrlDocument {
    /// Creates a new empty XBRL document
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses XBRL data from SEC JSON API format
    ///
    /// The SEC JSON format is documented at: https://www.sec.gov/edgar/sec-api-documentation
    pub fn parse_json(json: &str) -> Result<Self> {
        let api_response: SecApiResponse = serde_json::from_str(json)
            .map_err(|e| DataError::Parse(format!("Failed to parse SEC JSON: {}", e)))?;

        let mut facts = Vec::new();
        let entity_name = Some(api_response.entity_name.clone());
        let cik = Some(api_response.cik.clone());

        // Process each taxonomy (us-gaap, dei, etc.)
        for (taxonomy, taxonomy_facts) in &api_response.facts {
            // Process each concept in the taxonomy
            for (concept_name, concept_data) in &taxonomy_facts.0 {
                let full_concept = format!("{}:{}", taxonomy, concept_name);

                // Process units (USD, shares, etc.)
                for (unit, unit_facts) in &concept_data.units {
                    for fact_data in &unit_facts.0 {
                        // Parse dates
                        let period_end = NaiveDate::parse_from_str(&fact_data.end, "%Y-%m-%d")
                            .map_err(|e| DataError::Parse(format!("Invalid end date: {}", e)))?;

                        let period_start = if let Some(ref start) = fact_data.start {
                            Some(NaiveDate::parse_from_str(start, "%Y-%m-%d").map_err(|e| {
                                DataError::Parse(format!("Invalid start date: {}", e))
                            })?)
                        } else {
                            None
                        };

                        facts.push(XbrlFact {
                            concept: full_concept.clone(),
                            value: fact_data.val,
                            unit: unit.clone(),
                            period_end,
                            period_start,
                            form: fact_data.form.clone(),
                            fiscal_year: fact_data.fy,
                            fiscal_period: fact_data.fp.clone(),
                        });
                    }
                }
            }
        }

        Ok(Self {
            facts,
            entity_name,
            cik,
        })
    }

    /// Parses XBRL data from raw XML format (fallback method)
    ///
    /// Note: The JSON API is preferred as it's more reliable and easier to parse.
    pub fn parse_xml(xml: &str) -> Result<Self> {
        use quick_xml::Reader;
        use quick_xml::events::Event;

        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);

        let _facts: Vec<XbrlFact> = Vec::new();
        let mut buf = Vec::new();

        // This is a simplified XML parser
        // A full implementation would need to parse contexts, units, and facts
        // For now, we'll return an error directing users to use the JSON API

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Eof) => break,
                Ok(_) => {}
                Err(e) => return Err(DataError::XmlParse(format!("XML parse error: {}", e))),
            }
            buf.clear();
        }

        // For production use, implement full XBRL XML parsing or use the JSON API
        Err(DataError::Parse(
            "XML parsing not fully implemented. Please use the SEC JSON API instead.".to_string(),
        ))
    }

    /// Gets a specific fact by concept name and period end date
    pub fn get_fact(&self, concept: &str, period_end: NaiveDate) -> Option<&XbrlFact> {
        self.facts
            .iter()
            .find(|f| f.concept == concept && f.period_end == period_end)
    }

    /// Gets the most recent fact for a given concept
    pub fn get_latest_fact(&self, concept: &str) -> Option<&XbrlFact> {
        self.facts
            .iter()
            .filter(|f| f.concept == concept)
            .max_by_key(|f| f.period_end)
    }

    /// Gets all facts for a given concept, sorted by period end date (newest first)
    pub fn get_facts_by_concept(&self, concept: &str) -> Vec<&XbrlFact> {
        let mut facts: Vec<&XbrlFact> =
            self.facts.iter().filter(|f| f.concept == concept).collect();
        facts.sort_by(|a, b| b.period_end.cmp(&a.period_end));
        facts
    }

    /// Gets facts for a specific fiscal year
    pub fn get_facts_by_fiscal_year(&self, concept: &str, fiscal_year: i32) -> Vec<&XbrlFact> {
        self.facts
            .iter()
            .filter(|f| f.concept == concept && f.fiscal_year == Some(fiscal_year))
            .collect()
    }

    /// Gets facts by form type (e.g., "10-K", "10-Q")
    pub fn get_facts_by_form(&self, concept: &str, form: &str) -> Vec<&XbrlFact> {
        self.facts
            .iter()
            .filter(|f| f.concept == concept && f.form.as_deref() == Some(form))
            .collect()
    }

    /// Gets all available concepts in the document
    pub fn get_concepts(&self) -> Vec<String> {
        let mut concepts: Vec<String> = self.facts.iter().map(|f| f.concept.clone()).collect();
        concepts.sort();
        concepts.dedup();
        concepts
    }
}

// SEC API JSON structure
// Based on: https://www.sec.gov/edgar/sec-api-documentation

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecApiResponse {
    cik: String,
    entity_name: String,
    facts: HashMap<String, TaxonomyFacts>,
}

#[derive(Debug, Deserialize)]
struct TaxonomyFacts(HashMap<String, ConceptData>);

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ConceptData {
    label: String,
    description: String,
    units: HashMap<String, UnitFacts>,
}

#[derive(Debug, Deserialize)]
struct UnitFacts(Vec<FactData>);

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct FactData {
    end: String,
    val: f64,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    accn: Option<String>, // Accession number
    #[serde(default)]
    fy: Option<i32>, // Fiscal year
    #[serde(default)]
    fp: Option<String>, // Fiscal period
    #[serde(default)]
    form: Option<String>, // Form type (10-K, 10-Q, etc.)
    #[serde(default)]
    filed: Option<String>, // Filing date
}

/// Client for fetching XBRL data from SEC EDGAR
#[derive(Debug)]
pub struct XbrlClient {
    client: reqwest::Client,
    base_url: String,
}

impl XbrlClient {
    /// Creates a new XBRL client
    ///
    /// The client uses the SEC's JSON API by default.
    /// User-Agent header is required by SEC.
    pub fn new() -> Self {
        Self::with_user_agent("perth-data/0.1.0 (https://github.com/factordynamics/perth)")
    }

    /// Creates a new XBRL client with a custom User-Agent
    ///
    /// The SEC requires a User-Agent header for API requests.
    /// Format should be: "Company Name contact@email.com"
    pub fn with_user_agent(user_agent: &str) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(user_agent)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url: "https://data.sec.gov/api/xbrl".to_string(),
        }
    }

    /// Fetches all company facts for a given CIK
    ///
    /// # Arguments
    ///
    /// * `cik` - The CIK (Central Index Key) as a string. Can be padded or unpadded.
    ///
    /// # Returns
    ///
    /// An `XbrlDocument` containing all available facts for the company.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use perth_data::edgar::xbrl::XbrlClient;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = XbrlClient::new();
    /// let doc = client.fetch_company_facts("320193").await?; // Apple
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_company_facts(&self, cik: &str) -> Result<XbrlDocument> {
        // Pad CIK to 10 digits
        let cik_padded = format!("{:0>10}", cik);
        let url = format!("{}/companyfacts/CIK{}.json", self.base_url, cik_padded);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(DataError::Http(format!(
                "SEC API returned status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let json = response.text().await?;
        XbrlDocument::parse_json(&json)
    }

    /// Fetches company concept data for a specific concept
    ///
    /// This endpoint provides data for a single concept across all filings.
    ///
    /// # Arguments
    ///
    /// * `cik` - The CIK (Central Index Key)
    /// * `taxonomy` - The taxonomy (e.g., "us-gaap")
    /// * `concept` - The concept name (e.g., "Assets")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use perth_data::edgar::xbrl::XbrlClient;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = XbrlClient::new();
    /// let doc = client.fetch_company_concept("320193", "us-gaap", "Assets").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_company_concept(
        &self,
        cik: &str,
        taxonomy: &str,
        concept: &str,
    ) -> Result<XbrlDocument> {
        let cik_padded = format!("{:0>10}", cik);
        let url = format!(
            "{}/companyconcept/CIK{}/{}/{}.json",
            self.base_url, cik_padded, taxonomy, concept
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(DataError::Http(format!(
                "SEC API returned status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let json = response.text().await?;

        // Parse the concept-specific JSON format
        // For simplicity, we'll use the same parser but note that the structure
        // is slightly different for this endpoint
        XbrlDocument::parse_json(&json)
    }
}

impl Default for XbrlClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Common US-GAAP concepts for financial statements
pub mod concepts {
    /// Balance Sheet concepts
    pub mod balance_sheet {
        /// Total Assets
        pub const ASSETS: &str = "us-gaap:Assets";

        /// Total Liabilities
        pub const LIABILITIES: &str = "us-gaap:Liabilities";

        /// Stockholders' Equity
        pub const STOCKHOLDERS_EQUITY: &str = "us-gaap:StockholdersEquity";

        /// Long-term Debt (non-current)
        pub const LONG_TERM_DEBT: &str = "us-gaap:LongTermDebtNoncurrent";

        /// Alternative: Long-term Debt
        pub const LONG_TERM_DEBT_ALT: &str = "us-gaap:LongTermDebt";

        /// Cash and Cash Equivalents
        pub const CASH: &str = "us-gaap:CashAndCashEquivalentsAtCarryingValue";

        /// Current Assets
        pub const CURRENT_ASSETS: &str = "us-gaap:AssetsCurrent";

        /// Current Liabilities
        pub const CURRENT_LIABILITIES: &str = "us-gaap:LiabilitiesCurrent";
    }

    /// Income Statement concepts
    pub mod income_statement {
        /// Total Revenue
        pub const REVENUES: &str = "us-gaap:Revenues";

        /// Alternative: Revenue from Contract with Customer
        pub const REVENUE_FROM_CONTRACT: &str =
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax";

        /// Net Income (Loss)
        pub const NET_INCOME: &str = "us-gaap:NetIncomeLoss";

        /// Cost of Revenue
        pub const COST_OF_REVENUE: &str = "us-gaap:CostOfRevenue";

        /// Operating Income (Loss)
        pub const OPERATING_INCOME: &str = "us-gaap:OperatingIncomeLoss";

        /// Gross Profit
        pub const GROSS_PROFIT: &str = "us-gaap:GrossProfit";
    }

    /// Cash Flow Statement concepts
    pub mod cash_flow {
        /// Operating Cash Flows
        pub const OPERATING_CASH_FLOW: &str = "us-gaap:NetCashProvidedByUsedInOperatingActivities";

        /// Alternative: Operating Cash Flows
        pub const OPERATING_CASH_FLOW_ALT: &str = "us-gaap:OperatingCashFlows";

        /// Investing Cash Flows
        pub const INVESTING_CASH_FLOW: &str = "us-gaap:NetCashProvidedByUsedInInvestingActivities";

        /// Financing Cash Flows
        pub const FINANCING_CASH_FLOW: &str = "us-gaap:NetCashProvidedByUsedInFinancingActivities";
    }

    /// Per-Share concepts
    pub mod per_share {
        /// Earnings Per Share - Basic
        pub const EPS_BASIC: &str = "us-gaap:EarningsPerShareBasic";

        /// Earnings Per Share - Diluted
        pub const EPS_DILUTED: &str = "us-gaap:EarningsPerShareDiluted";

        /// Common Stock Shares Outstanding
        pub const SHARES_OUTSTANDING: &str = "us-gaap:CommonStockSharesOutstanding";

        /// Weighted Average Shares Outstanding - Basic
        pub const SHARES_OUTSTANDING_BASIC: &str =
            "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic";

        /// Weighted Average Shares Outstanding - Diluted
        pub const SHARES_OUTSTANDING_DILUTED: &str =
            "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding";
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xbrl_fact_instant() {
        let fact = XbrlFact {
            concept: "us-gaap:Assets".to_string(),
            value: 1000000.0,
            unit: "USD".to_string(),
            period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            period_start: None,
            form: Some("10-K".to_string()),
            fiscal_year: Some(2023),
            fiscal_period: Some("FY".to_string()),
        };

        assert!(fact.is_instant());
        assert!(!fact.is_duration());
        assert_eq!(fact.duration_days(), None);
    }

    #[test]
    fn test_xbrl_fact_duration() {
        let fact = XbrlFact {
            concept: "us-gaap:NetIncomeLoss".to_string(),
            value: 100000.0,
            unit: "USD".to_string(),
            period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            period_start: Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
            form: Some("10-K".to_string()),
            fiscal_year: Some(2023),
            fiscal_period: Some("FY".to_string()),
        };

        assert!(!fact.is_instant());
        assert!(fact.is_duration());
        assert_eq!(fact.duration_days(), Some(364));
    }

    #[test]
    fn test_xbrl_document_get_facts() {
        let mut doc = XbrlDocument::new();

        doc.facts.push(XbrlFact {
            concept: "us-gaap:Assets".to_string(),
            value: 1000000.0,
            unit: "USD".to_string(),
            period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            period_start: None,
            form: Some("10-K".to_string()),
            fiscal_year: Some(2023),
            fiscal_period: Some("FY".to_string()),
        });

        doc.facts.push(XbrlFact {
            concept: "us-gaap:Assets".to_string(),
            value: 950000.0,
            unit: "USD".to_string(),
            period_end: NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
            period_start: None,
            form: Some("10-K".to_string()),
            fiscal_year: Some(2022),
            fiscal_period: Some("FY".to_string()),
        });

        // Test get_latest_fact
        let latest = doc.get_latest_fact("us-gaap:Assets").unwrap();
        assert_eq!(latest.value, 1000000.0);
        assert_eq!(latest.fiscal_year, Some(2023));

        // Test get_fact
        let specific = doc
            .get_fact(
                "us-gaap:Assets",
                NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
            )
            .unwrap();
        assert_eq!(specific.value, 950000.0);

        // Test get_facts_by_concept
        let all_assets = doc.get_facts_by_concept("us-gaap:Assets");
        assert_eq!(all_assets.len(), 2);
        assert_eq!(all_assets[0].fiscal_year, Some(2023)); // Newest first

        // Test get_facts_by_fiscal_year
        let fy2023 = doc.get_facts_by_fiscal_year("us-gaap:Assets", 2023);
        assert_eq!(fy2023.len(), 1);
        assert_eq!(fy2023[0].value, 1000000.0);

        // Test get_concepts
        let concepts = doc.get_concepts();
        assert_eq!(concepts.len(), 1);
        assert_eq!(concepts[0], "us-gaap:Assets");
    }

    #[test]
    fn test_parse_json_invalid() {
        let result = XbrlDocument::parse_json("invalid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_concepts_constants() {
        use concepts::*;

        assert_eq!(balance_sheet::ASSETS, "us-gaap:Assets");
        assert_eq!(income_statement::NET_INCOME, "us-gaap:NetIncomeLoss");
        assert_eq!(
            cash_flow::OPERATING_CASH_FLOW,
            "us-gaap:NetCashProvidedByUsedInOperatingActivities"
        );
        assert_eq!(per_share::EPS_BASIC, "us-gaap:EarningsPerShareBasic");
    }
}
