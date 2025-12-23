//! SEC EDGAR data fetching and parsing.
//!
//! This module provides access to SEC EDGAR filings including:
//! - Company CIK lookup from ticker symbols
//! - 10-K and 10-Q filing retrieval
//! - XBRL parsing for financial data extraction
//! - Fundamental data extraction for factor calculations
//!
//! # Example
//!
//! ```no_run
//! use perth_data::edgar::{EdgarClient, XbrlClient, EdgarFundamentalsProvider};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Look up company CIK and fetch filings
//!     let client = EdgarClient::new()?;
//!     let cik = client.get_company_cik("AAPL").await?;
//!     let filings = client.get_company_filings(&cik).await?;
//!     println!("Found {} recent filings", filings.filings.recent.accession_number.len());
//!
//!     // Fetch XBRL data directly
//!     let xbrl = XbrlClient::new();
//!     let doc = xbrl.fetch_company_facts(&cik).await?;
//!     if let Some(fact) = doc.get_latest_fact("us-gaap:Assets") {
//!         println!("Total Assets: {} {}", fact.value, fact.unit);
//!     }
//!
//!     // Get structured financial statements
//!     let provider = EdgarFundamentalsProvider::new();
//!     let financials = provider.fetch_financials("AAPL").await?;
//!     println!("Found {} financial statements", financials.len());
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod filings;
pub mod fundamentals;
pub mod xbrl;

// Re-export main types
pub use client::{
    CompanyFilings as EdgarCompanyFilings, EdgarClient, FilingsContainer, FilingsRecent,
};
pub use filings::{CikLookup, CompanyFilings, FilingHistory, FilingInfo, RecentFilings};
pub use fundamentals::{EdgarFundamentalsProvider, FactorInputs, FinancialStatement, PeriodType};
pub use xbrl::{XbrlClient, XbrlDocument, XbrlFact, concepts};
