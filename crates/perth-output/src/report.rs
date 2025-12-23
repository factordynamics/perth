//! Report generation for Perth factor model.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during report generation.
#[derive(Debug, Error)]
pub enum ReportError {
    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// A report from the Perth factor model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    /// Symbol being analyzed.
    pub symbol: String,

    /// Report generation timestamp.
    pub timestamp: DateTime<Utc>,

    /// Analysis period in years.
    pub period_years: u32,

    /// Report contents (JSON format).
    pub contents: serde_json::Value,
}

impl Report {
    /// Create a new report.
    pub fn new(symbol: String, period_years: u32, contents: serde_json::Value) -> Self {
        Self {
            symbol,
            timestamp: Utc::now(),
            period_years,
            contents,
        }
    }

    /// Convert report to JSON string.
    pub fn to_json(&self) -> Result<String, ReportError> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

/// Builder for creating reports.
#[derive(Debug, Default)]
pub struct ReportBuilder {
    symbol: Option<String>,
    period_years: Option<u32>,
    contents: Option<serde_json::Value>,
}

impl ReportBuilder {
    /// Create a new report builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the symbol.
    pub fn symbol(mut self, symbol: String) -> Self {
        self.symbol = Some(symbol);
        self
    }

    /// Set the analysis period.
    pub const fn period_years(mut self, years: u32) -> Self {
        self.period_years = Some(years);
        self
    }

    /// Set the report contents.
    pub fn contents(mut self, contents: serde_json::Value) -> Self {
        self.contents = Some(contents);
        self
    }

    /// Build the report.
    pub fn build(self) -> Result<Report, ReportError> {
        Ok(Report::new(
            self.symbol.unwrap_or_default(),
            self.period_years.unwrap_or(5),
            self.contents.unwrap_or(serde_json::Value::Null),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_creation() {
        let report = Report::new("AAPL".to_string(), 5, serde_json::json!({"test": "data"}));

        assert_eq!(report.symbol, "AAPL");
        assert_eq!(report.period_years, 5);
    }

    #[test]
    fn test_report_builder() {
        let report = ReportBuilder::new()
            .symbol("MSFT".to_string())
            .period_years(3)
            .contents(serde_json::json!({"key": "value"}))
            .build()
            .unwrap();

        assert_eq!(report.symbol, "MSFT");
        assert_eq!(report.period_years, 3);
    }
}
