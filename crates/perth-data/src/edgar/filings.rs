//! SEC EDGAR filings API integration.
//!
//! This module provides functionality to:
//! - Look up CIK numbers by ticker symbols
//! - Fetch company filing history from SEC EDGAR
//! - Filter and extract specific filing types (10-K, 10-Q)

use crate::error::{DataError, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use std::collections::HashMap;

/// Lookup table for converting ticker symbols to CIK numbers.
///
/// CIK (Central Index Key) is a unique identifier assigned by the SEC to
/// companies filing with EDGAR.
#[derive(Debug, Clone)]
pub struct CikLookup {
    /// Map from ticker to (CIK, company name)
    ticker_to_cik: HashMap<String, (String, String)>,
}

/// Raw company ticker data from SEC JSON.
#[derive(Debug, Deserialize)]
struct CompanyTicker {
    cik_str: u64,
    ticker: String,
    title: String,
}

impl CikLookup {
    /// Fetch and parse the company tickers JSON from SEC.
    ///
    /// Downloads the latest ticker-to-CIK mapping from the SEC website.
    /// This includes all companies with public filings.
    ///
    /// # Arguments
    /// * `client` - HTTP client for making requests
    ///
    /// # Returns
    /// A CikLookup instance containing all ticker mappings
    ///
    /// # Errors
    /// Returns error if network request fails or JSON parsing fails
    pub async fn fetch(client: &reqwest::Client) -> Result<Self> {
        let url = "https://www.sec.gov/files/company_tickers.json";

        // SEC requires a User-Agent header with contact info
        let response = client
            .get(url)
            .header(
                "User-Agent",
                "Perth-FactorModel/0.1.0 (perth@factordynamics.io)",
            )
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(DataError::Http(format!(
                "Failed to fetch company tickers: HTTP {}",
                response.status()
            )));
        }

        // Parse the JSON response - it's a map from index to company data
        let data: HashMap<String, CompanyTicker> = response.json().await?;

        // Build the ticker to CIK mapping
        let mut ticker_to_cik = HashMap::new();
        for (_idx, company) in data {
            let cik = company.cik_str.to_string();
            let ticker = company.ticker.to_uppercase();
            let name = company.title;
            ticker_to_cik.insert(ticker, (cik, name));
        }

        Ok(Self { ticker_to_cik })
    }

    /// Look up CIK by ticker symbol.
    ///
    /// # Arguments
    /// * `ticker` - The stock ticker symbol (case-insensitive)
    ///
    /// # Returns
    /// Optional tuple of (CIK, company name)
    pub fn get_cik(&self, ticker: &str) -> Option<&(String, String)> {
        self.ticker_to_cik.get(&ticker.to_uppercase())
    }

    /// Get all tickers in the lookup table.
    ///
    /// # Returns
    /// Vector of all ticker symbols
    pub fn all_tickers(&self) -> Vec<&str> {
        self.ticker_to_cik.keys().map(|s| s.as_str()).collect()
    }

    /// Pad CIK to 10 digits as required by SEC.
    ///
    /// SEC EDGAR URLs require CIKs to be zero-padded to 10 digits.
    ///
    /// # Arguments
    /// * `cik` - The CIK number as a string
    ///
    /// # Returns
    /// Zero-padded CIK string
    ///
    /// # Example
    /// ```
    /// # use perth_data::edgar::filings::CikLookup;
    /// let padded = CikLookup::pad_cik("320193");
    /// assert_eq!(padded, "0000320193");
    /// ```
    pub fn pad_cik(cik: &str) -> String {
        format!("{:0>10}", cik)
    }
}

/// Company filings data from SEC EDGAR submissions API.
#[derive(Debug, Clone, Deserialize)]
pub struct CompanyFilings {
    /// Central Index Key
    pub cik: String,
    /// Company name
    pub name: String,
    /// Filing history
    pub filings: FilingHistory,
}

/// Container for filing history data.
#[derive(Debug, Clone, Deserialize)]
pub struct FilingHistory {
    /// Recent filings
    pub recent: RecentFilings,
}

/// Recent filings data.
///
/// The SEC API returns filing information as parallel arrays where
/// each index corresponds to a single filing.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecentFilings {
    /// Accession numbers (unique filing identifiers)
    pub accession_number: Vec<String>,
    /// Form types (e.g., "10-K", "10-Q", "8-K")
    #[serde(rename = "form")]
    pub form: Vec<String>,
    /// Filing dates in YYYY-MM-DD format
    pub filing_date: Vec<String>,
    /// Primary document filenames
    pub primary_document: Vec<String>,
}

/// Information about a specific filing.
#[derive(Debug, Clone)]
pub struct FilingInfo {
    /// Accession number (unique filing identifier)
    pub accession_number: String,
    /// Form type (e.g., "10-K", "10-Q")
    pub form: String,
    /// Filing date
    pub filing_date: NaiveDate,
    /// Primary document filename
    pub primary_document: String,
}

impl CompanyFilings {
    /// Fetch company filings from SEC EDGAR submissions API.
    ///
    /// # Arguments
    /// * `client` - HTTP client for making requests
    /// * `cik` - Central Index Key (will be padded to 10 digits)
    ///
    /// # Returns
    /// CompanyFilings containing the filing history
    ///
    /// # Errors
    /// Returns error if network request fails or JSON parsing fails
    pub async fn fetch(client: &reqwest::Client, cik: &str) -> Result<Self> {
        let padded_cik = CikLookup::pad_cik(cik);
        let url = format!("https://data.sec.gov/submissions/CIK{}.json", padded_cik);

        let response = client
            .get(&url)
            .header("User-Agent", "Perth Factor Model/1.0")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(DataError::Http(format!(
                "Failed to fetch filings for CIK {}: HTTP {}",
                cik,
                response.status()
            )));
        }

        let filings: Self = response.json().await?;
        Ok(filings)
    }

    /// Get the most recent 10-K filing.
    ///
    /// 10-K forms are annual reports that provide comprehensive overview
    /// of a company's financial performance.
    ///
    /// # Returns
    /// Optional FilingInfo for the most recent 10-K
    pub fn latest_10k(&self) -> Option<FilingInfo> {
        self.find_latest_by_form("10-K")
    }

    /// Get the most recent 10-Q filing.
    ///
    /// 10-Q forms are quarterly reports filed by public companies.
    ///
    /// # Returns
    /// Optional FilingInfo for the most recent 10-Q
    pub fn latest_10q(&self) -> Option<FilingInfo> {
        self.find_latest_by_form("10-Q")
    }

    /// Get all 10-K filings.
    ///
    /// # Returns
    /// Vector of all 10-K filings, sorted by date (most recent first)
    pub fn all_10k(&self) -> Vec<FilingInfo> {
        self.find_all_by_form("10-K")
    }

    /// Get all 10-Q filings.
    ///
    /// # Returns
    /// Vector of all 10-Q filings, sorted by date (most recent first)
    pub fn all_10q(&self) -> Vec<FilingInfo> {
        self.find_all_by_form("10-Q")
    }

    /// Find the most recent filing of a specific form type.
    fn find_latest_by_form(&self, form_type: &str) -> Option<FilingInfo> {
        let recent = &self.filings.recent;

        // Find the first matching form (they're already sorted by date, most recent first)
        for i in 0..recent.form.len() {
            if recent.form[i] == form_type {
                return self.filing_at_index(i).ok();
            }
        }

        None
    }

    /// Find all filings of a specific form type.
    fn find_all_by_form(&self, form_type: &str) -> Vec<FilingInfo> {
        let recent = &self.filings.recent;
        let mut filings = Vec::new();

        for i in 0..recent.form.len() {
            if recent.form[i] == form_type
                && let Ok(filing) = self.filing_at_index(i)
            {
                filings.push(filing);
            }
        }

        filings
    }

    /// Extract filing information at a specific index.
    fn filing_at_index(&self, idx: usize) -> Result<FilingInfo> {
        let recent = &self.filings.recent;

        // Parse the filing date
        let filing_date = NaiveDate::parse_from_str(&recent.filing_date[idx], "%Y-%m-%d")
            .map_err(|e| DataError::Parse(format!("Invalid filing date: {}", e)))?;

        Ok(FilingInfo {
            accession_number: recent.accession_number[idx].clone(),
            form: recent.form[idx].clone(),
            filing_date,
            primary_document: recent.primary_document[idx].clone(),
        })
    }
}

impl FilingInfo {
    /// Get the URL to the primary document for this filing.
    ///
    /// # Arguments
    /// * `cik` - Central Index Key for the company
    ///
    /// # Returns
    /// Full URL to the document on SEC EDGAR
    ///
    /// # Example
    /// ```no_run
    /// # use perth_data::edgar::filings::FilingInfo;
    /// # use chrono::NaiveDate;
    /// let filing = FilingInfo {
    ///     accession_number: "0000320193-23-000077".to_string(),
    ///     form: "10-K".to_string(),
    ///     filing_date: NaiveDate::from_ymd_opt(2023, 11, 3).unwrap(),
    ///     primary_document: "aapl-20230930.htm".to_string(),
    /// };
    /// let url = filing.document_url("320193");
    /// assert!(url.contains("edgar/data/320193"));
    /// ```
    pub fn document_url(&self, cik: &str) -> String {
        // Remove dashes from accession number for the URL path
        let accession_no_dashes = self.accession_number.replace("-", "");

        format!(
            "https://www.sec.gov/Archives/edgar/data/{}/{}/{}",
            cik, accession_no_dashes, self.primary_document
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pad_cik() {
        assert_eq!(CikLookup::pad_cik("320193"), "0000320193");
        assert_eq!(CikLookup::pad_cik("1234"), "0000001234");
        assert_eq!(CikLookup::pad_cik("1234567890"), "1234567890");
    }

    #[test]
    fn test_document_url() {
        let filing = FilingInfo {
            accession_number: "0000320193-23-000077".to_string(),
            form: "10-K".to_string(),
            filing_date: NaiveDate::from_ymd_opt(2023, 11, 3).unwrap(),
            primary_document: "aapl-20230930.htm".to_string(),
        };

        let url = filing.document_url("320193");
        assert_eq!(
            url,
            "https://www.sec.gov/Archives/edgar/data/320193/000032019323000077/aapl-20230930.htm"
        );
    }

    #[tokio::test]
    async fn test_fetch_cik_lookup() {
        let client = reqwest::Client::builder()
            .user_agent("Perth Factor Model/1.0 (test)")
            .build()
            .unwrap();
        let lookup = CikLookup::fetch(&client).await;

        assert!(
            lookup.is_ok(),
            "Failed to fetch CIK lookup: {:?}",
            lookup.err()
        );
        let lookup = lookup.unwrap();

        // Test known tickers
        assert!(lookup.get_cik("AAPL").is_some());
        assert!(lookup.get_cik("MSFT").is_some());

        // Test case insensitivity
        let apple_cik = lookup.get_cik("aapl");
        assert!(apple_cik.is_some());
        let (cik, name) = apple_cik.unwrap();
        assert_eq!(cik, "320193");
        assert!(name.contains("Apple") || name.contains("APPLE"));
    }

    #[tokio::test]
    async fn test_fetch_company_filings() {
        let client = reqwest::Client::builder()
            .user_agent("Perth Factor Model/1.0 (test)")
            .build()
            .unwrap();

        // Fetch Apple's filings (CIK: 320193)
        let filings = CompanyFilings::fetch(&client, "320193").await;

        assert!(filings.is_ok());
        let filings = filings.unwrap();

        // CIK is stored as returned by SEC API (may be unpadded or padded)
        assert!(filings.cik.parse::<u64>().is_ok());
        assert!(filings.cik.contains("320193"));
        assert!(!filings.name.is_empty());
        assert!(!filings.filings.recent.form.is_empty());

        // Test getting 10-K and 10-Q filings
        let latest_10k = filings.latest_10k();
        assert!(latest_10k.is_some());

        let latest_10q = filings.latest_10q();
        assert!(latest_10q.is_some());

        let all_10k = filings.all_10k();
        assert!(!all_10k.is_empty());

        let all_10q = filings.all_10q();
        assert!(!all_10q.is_empty());
    }
}
