//! Error types for data operations.

use thiserror::Error;

/// Result type for data operations.
pub type Result<T> = std::result::Result<T, DataError>;

/// Errors that can occur during data operations.
#[derive(Debug, Error)]
pub enum DataError {
    /// Yahoo Finance API error
    #[error("Yahoo Finance API error: {0}")]
    YahooApi(String),

    /// Network error
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    /// Database error
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    /// Data parsing error
    #[error("Data parsing error: {0}")]
    Parse(String),

    /// Invalid date range
    #[error("Invalid date range: start {start} is after end {end}")]
    InvalidDateRange {
        /// Start date of the range
        start: String,
        /// End date of the range
        end: String,
    },

    /// Missing data
    #[error("Missing data for {symbol}: {reason}")]
    MissingData {
        /// Symbol that was queried
        symbol: String,
        /// Reason for missing data
        reason: String,
    },

    /// Polars error
    #[error("Polars error: {0}")]
    Polars(#[from] polars::prelude::PolarsError),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Time conversion error
    #[error("Time conversion error: {0}")]
    TimeConversion(String),

    /// Rate limit error
    #[error("Rate limit exceeded, please retry after {retry_after_ms}ms")]
    RateLimit {
        /// Milliseconds to wait before retrying
        retry_after_ms: u64,
    },

    /// Invalid symbol
    #[error("Invalid symbol: {0}")]
    InvalidSymbol(String),

    /// Cache error
    #[error("Cache error: {0}")]
    Cache(String),

    /// XML parsing error
    #[error("XML parsing error: {0}")]
    XmlParse(String),

    /// HTTP error
    #[error("HTTP error: {0}")]
    Http(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// SEC EDGAR API error
    #[error("EDGAR API error: {0}")]
    EdgarApi(String),

    /// CIK not found for ticker
    #[error("CIK not found for ticker: {0}")]
    CikNotFound(String),

    /// XBRL parsing error
    #[error("XBRL parsing error: {0}")]
    XbrlParse(String),

    /// Filing not found
    #[error("Filing not found: {0}")]
    FilingNotFound(String),
}

impl From<yahoo_finance_api::YahooError> for DataError {
    fn from(err: yahoo_finance_api::YahooError) -> Self {
        Self::YahooApi(err.to_string())
    }
}
