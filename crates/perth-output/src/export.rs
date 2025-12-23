//! Export functionality for Perth factor model data.
//!
//! This module provides comprehensive CSV and JSON export capabilities for
//! factor exposures, risk decomposition, and portfolio analysis.

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use thiserror::Error;

/// Errors that can occur during export operations.
#[derive(Debug, Error)]
pub enum ExportError {
    /// CSV serialization error.
    #[error("CSV serialization error: {0}")]
    Csv(#[from] csv::Error),

    /// JSON serialization error.
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid format error.
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}

/// Export format options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// Comma-separated values format.
    Csv,

    /// Compact JSON format.
    Json,

    /// Pretty-printed JSON format.
    PrettyJson,
}

impl ExportFormat {
    /// Get the file extension for this format.
    pub const fn extension(&self) -> &str {
        match self {
            Self::Csv => "csv",
            Self::Json | Self::PrettyJson => "json",
        }
    }
}

/// Factor exposure data for a single security at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FactorExposureExport {
    /// Security symbol.
    pub symbol: String,

    /// Date of the exposure.
    pub date: NaiveDate,

    /// Name of the factor.
    pub factor_name: String,

    /// Raw exposure value.
    pub exposure: f64,

    /// Standardized z-score of the exposure.
    pub z_score: f64,
}

impl FactorExposureExport {
    /// Create a new factor exposure export.
    pub const fn new(
        symbol: String,
        date: NaiveDate,
        factor_name: String,
        exposure: f64,
        z_score: f64,
    ) -> Self {
        Self {
            symbol,
            date,
            factor_name,
            exposure,
            z_score,
        }
    }
}

/// Risk decomposition data for a single security.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskDecompositionExport {
    /// Security symbol.
    pub symbol: String,

    /// Total risk (volatility).
    pub total_risk: f64,

    /// Factor-related risk component.
    pub factor_risk: f64,

    /// Security-specific (idiosyncratic) risk component.
    pub specific_risk: f64,

    /// Individual factor contributions to total risk.
    pub factor_contributions: HashMap<String, f64>,
}

impl RiskDecompositionExport {
    /// Create a new risk decomposition export.
    pub const fn new(
        symbol: String,
        total_risk: f64,
        factor_risk: f64,
        specific_risk: f64,
        factor_contributions: HashMap<String, f64>,
    ) -> Self {
        Self {
            symbol,
            total_risk,
            factor_risk,
            specific_risk,
            factor_contributions,
        }
    }

    /// Convert to a flat structure suitable for CSV export.
    fn to_flat_records(&self) -> Vec<RiskDecompositionFlat> {
        let mut records = Vec::new();

        // Add main risk metrics
        records.push(RiskDecompositionFlat {
            symbol: self.symbol.clone(),
            risk_type: "total".to_string(),
            value: self.total_risk,
        });

        records.push(RiskDecompositionFlat {
            symbol: self.symbol.clone(),
            risk_type: "factor".to_string(),
            value: self.factor_risk,
        });

        records.push(RiskDecompositionFlat {
            symbol: self.symbol.clone(),
            risk_type: "specific".to_string(),
            value: self.specific_risk,
        });

        // Add factor contributions
        for (factor, contribution) in &self.factor_contributions {
            records.push(RiskDecompositionFlat {
                symbol: self.symbol.clone(),
                risk_type: format!("factor_{}", factor),
                value: *contribution,
            });
        }

        records
    }
}

/// Flattened risk decomposition for CSV export.
#[derive(Debug, Serialize, Deserialize)]
struct RiskDecompositionFlat {
    symbol: String,
    risk_type: String,
    value: f64,
}

/// Portfolio export data containing multiple securities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortfolioExport {
    /// Portfolio name or identifier.
    pub name: String,

    /// Date of the portfolio snapshot.
    pub date: NaiveDate,

    /// Securities in the portfolio with their weights.
    pub holdings: Vec<PortfolioHolding>,
}

impl PortfolioExport {
    /// Create a new portfolio export.
    pub const fn new(name: String, date: NaiveDate, holdings: Vec<PortfolioHolding>) -> Self {
        Self {
            name,
            date,
            holdings,
        }
    }

    /// Get total portfolio weight (should be close to 1.0).
    pub fn total_weight(&self) -> f64 {
        self.holdings.iter().map(|h| h.weight).sum()
    }
}

/// A single holding in a portfolio.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortfolioHolding {
    /// Security symbol.
    pub symbol: String,

    /// Weight in the portfolio (0.0 to 1.0).
    pub weight: f64,

    /// Market value of the holding.
    pub market_value: Option<f64>,

    /// Number of shares.
    pub shares: Option<f64>,
}

impl PortfolioHolding {
    /// Create a new portfolio holding.
    pub const fn new(
        symbol: String,
        weight: f64,
        market_value: Option<f64>,
        shares: Option<f64>,
    ) -> Self {
        Self {
            symbol,
            weight,
            market_value,
            shares,
        }
    }
}

/// Trait for exporting data in various formats.
pub trait Exporter {
    /// Export data to a string in the specified format.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn export_to_string(&self, format: ExportFormat) -> Result<String, ExportError>;

    /// Export data to a file in the specified format.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or file writing fails.
    fn export_to_file(&self, path: &Path, format: ExportFormat) -> Result<(), ExportError> {
        let content = self.export_to_string(format)?;
        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }
}

impl Exporter for FactorExposureExport {
    fn export_to_string(&self, format: ExportFormat) -> Result<String, ExportError> {
        match format {
            ExportFormat::Csv => {
                let mut wtr = csv::Writer::from_writer(vec![]);
                wtr.serialize(self)?;
                let data =
                    String::from_utf8(wtr.into_inner().map_err(|e| e.into_error())?).unwrap();
                Ok(data)
            }
            ExportFormat::Json => Ok(serde_json::to_string(self)?),
            ExportFormat::PrettyJson => Ok(serde_json::to_string_pretty(self)?),
        }
    }
}

impl Exporter for Vec<FactorExposureExport> {
    fn export_to_string(&self, format: ExportFormat) -> Result<String, ExportError> {
        match format {
            ExportFormat::Csv => {
                let mut wtr = csv::Writer::from_writer(vec![]);
                for record in self {
                    wtr.serialize(record)?;
                }
                let data =
                    String::from_utf8(wtr.into_inner().map_err(|e| e.into_error())?).unwrap();
                Ok(data)
            }
            ExportFormat::Json => Ok(serde_json::to_string(self)?),
            ExportFormat::PrettyJson => Ok(serde_json::to_string_pretty(self)?),
        }
    }
}

impl Exporter for RiskDecompositionExport {
    fn export_to_string(&self, format: ExportFormat) -> Result<String, ExportError> {
        match format {
            ExportFormat::Csv => {
                let records = self.to_flat_records();
                let mut wtr = csv::Writer::from_writer(vec![]);
                for record in records {
                    wtr.serialize(&record)?;
                }
                let data =
                    String::from_utf8(wtr.into_inner().map_err(|e| e.into_error())?).unwrap();
                Ok(data)
            }
            ExportFormat::Json => Ok(serde_json::to_string(self)?),
            ExportFormat::PrettyJson => Ok(serde_json::to_string_pretty(self)?),
        }
    }
}

impl Exporter for Vec<RiskDecompositionExport> {
    fn export_to_string(&self, format: ExportFormat) -> Result<String, ExportError> {
        match format {
            ExportFormat::Csv => {
                let mut wtr = csv::Writer::from_writer(vec![]);
                for decomp in self {
                    for record in decomp.to_flat_records() {
                        wtr.serialize(&record)?;
                    }
                }
                let data =
                    String::from_utf8(wtr.into_inner().map_err(|e| e.into_error())?).unwrap();
                Ok(data)
            }
            ExportFormat::Json => Ok(serde_json::to_string(self)?),
            ExportFormat::PrettyJson => Ok(serde_json::to_string_pretty(self)?),
        }
    }
}

impl Exporter for PortfolioExport {
    fn export_to_string(&self, format: ExportFormat) -> Result<String, ExportError> {
        match format {
            ExportFormat::Csv => {
                let mut output = String::new();

                // Write header information as comments
                output.push_str(&format!("# Portfolio: {}\n", self.name));
                output.push_str(&format!("# Date: {}\n", self.date));
                output.push_str(&format!("# Total Weight: {}\n", self.total_weight()));

                // Write holdings as CSV
                let mut wtr = csv::Writer::from_writer(vec![]);
                wtr.write_record(["symbol", "weight", "market_value", "shares"])?;
                for holding in &self.holdings {
                    wtr.write_record([
                        &holding.symbol,
                        &holding.weight.to_string(),
                        &holding
                            .market_value
                            .map(|v| v.to_string())
                            .unwrap_or_default(),
                        &holding.shares.map(|s| s.to_string()).unwrap_or_default(),
                    ])?;
                }
                let holdings_data =
                    String::from_utf8(wtr.into_inner().map_err(|e| e.into_error())?).unwrap();
                output.push_str(&holdings_data);
                Ok(output)
            }
            ExportFormat::Json => Ok(serde_json::to_string(self)?),
            ExportFormat::PrettyJson => Ok(serde_json::to_string_pretty(self)?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_factor_exposure_export_csv() {
        let exposure = FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.75,
            1.5,
        );

        let csv = exposure.export_to_string(ExportFormat::Csv).unwrap();
        assert!(csv.contains("AAPL"));
        assert!(csv.contains("momentum"));
        assert!(csv.contains("0.75"));
        assert!(csv.contains("1.5"));
    }

    #[test]
    fn test_factor_exposure_export_json() {
        let exposure = FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.75,
            1.5,
        );

        let json = exposure.export_to_string(ExportFormat::Json).unwrap();
        assert!(json.contains("\"AAPL\""));
        assert!(json.contains("\"momentum\""));
        assert!(json.contains("0.75"));
        assert!(json.contains("1.5"));
    }

    #[test]
    fn test_factor_exposure_export_pretty_json() {
        let exposure = FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.75,
            1.5,
        );

        let json = exposure.export_to_string(ExportFormat::PrettyJson).unwrap();
        assert!(json.contains("\"AAPL\""));
        assert!(json.contains("  ")); // Indentation indicates pretty format
    }

    #[test]
    fn test_multiple_factor_exposures_csv() {
        let exposures = vec![
            FactorExposureExport::new(
                "AAPL".to_string(),
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                "momentum".to_string(),
                0.75,
                1.5,
            ),
            FactorExposureExport::new(
                "MSFT".to_string(),
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                "value".to_string(),
                -0.5,
                -1.0,
            ),
        ];

        let csv = exposures.export_to_string(ExportFormat::Csv).unwrap();
        assert!(csv.contains("AAPL"));
        assert!(csv.contains("MSFT"));
        assert!(csv.contains("momentum"));
        assert!(csv.contains("value"));
    }

    #[test]
    fn test_risk_decomposition_export_csv() {
        let mut contributions = HashMap::new();
        contributions.insert("momentum".to_string(), 0.15);
        contributions.insert("value".to_string(), 0.10);

        let risk =
            RiskDecompositionExport::new("AAPL".to_string(), 0.25, 0.20, 0.05, contributions);

        let csv = risk.export_to_string(ExportFormat::Csv).unwrap();
        assert!(csv.contains("AAPL"));
        assert!(csv.contains("total"));
        assert!(csv.contains("factor"));
        assert!(csv.contains("specific"));
        assert!(csv.contains("factor_momentum"));
        assert!(csv.contains("factor_value"));
    }

    #[test]
    fn test_risk_decomposition_export_json() {
        let mut contributions = HashMap::new();
        contributions.insert("momentum".to_string(), 0.15);

        let risk =
            RiskDecompositionExport::new("AAPL".to_string(), 0.25, 0.20, 0.05, contributions);

        let json = risk.export_to_string(ExportFormat::Json).unwrap();
        assert!(json.contains("\"AAPL\""));
        assert!(json.contains("\"total_risk\""));
        assert!(json.contains("\"factor_risk\""));
        assert!(json.contains("\"specific_risk\""));
        assert!(json.contains("\"factor_contributions\""));
    }

    #[test]
    fn test_portfolio_export_csv() {
        let holdings = vec![
            PortfolioHolding::new("AAPL".to_string(), 0.4, Some(40000.0), Some(100.0)),
            PortfolioHolding::new("MSFT".to_string(), 0.3, Some(30000.0), Some(75.0)),
            PortfolioHolding::new("GOOGL".to_string(), 0.3, Some(30000.0), Some(50.0)),
        ];

        let portfolio = PortfolioExport::new(
            "Tech Portfolio".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            holdings,
        );

        let csv = portfolio.export_to_string(ExportFormat::Csv).unwrap();
        assert!(csv.contains("Tech Portfolio"));
        assert!(csv.contains("AAPL"));
        assert!(csv.contains("MSFT"));
        assert!(csv.contains("GOOGL"));
        assert!(csv.contains("0.4"));
        assert!(csv.contains("40000"));
    }

    #[test]
    fn test_portfolio_export_json() {
        let holdings = vec![
            PortfolioHolding::new("AAPL".to_string(), 0.5, Some(50000.0), None),
            PortfolioHolding::new("MSFT".to_string(), 0.5, Some(50000.0), None),
        ];

        let portfolio = PortfolioExport::new(
            "Balanced Portfolio".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            holdings,
        );

        let json = portfolio.export_to_string(ExportFormat::Json).unwrap();
        assert!(json.contains("\"Balanced Portfolio\""));
        assert!(json.contains("\"AAPL\""));
        assert!(json.contains("\"MSFT\""));
    }

    #[test]
    fn test_portfolio_total_weight() {
        let holdings = vec![
            PortfolioHolding::new("AAPL".to_string(), 0.4, None, None),
            PortfolioHolding::new("MSFT".to_string(), 0.35, None, None),
            PortfolioHolding::new("GOOGL".to_string(), 0.25, None, None),
        ];

        let portfolio = PortfolioExport::new(
            "Test Portfolio".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            holdings,
        );

        assert_eq!(portfolio.total_weight(), 1.0);
    }

    #[test]
    fn test_export_to_file() {
        use std::io::Read;

        let exposure = FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.75,
            1.5,
        );

        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_export.csv");
        let json_path = temp_dir.join("test_export.json");

        // Test CSV export
        exposure
            .export_to_file(&csv_path, ExportFormat::Csv)
            .unwrap();
        let mut csv_content = String::new();
        File::open(&csv_path)
            .unwrap()
            .read_to_string(&mut csv_content)
            .unwrap();
        assert!(csv_content.contains("AAPL"));

        // Test JSON export
        exposure
            .export_to_file(&json_path, ExportFormat::Json)
            .unwrap();
        let mut json_content = String::new();
        File::open(&json_path)
            .unwrap()
            .read_to_string(&mut json_content)
            .unwrap();
        assert!(json_content.contains("\"AAPL\""));

        // Clean up
        std::fs::remove_file(csv_path).ok();
        std::fs::remove_file(json_path).ok();
    }

    #[test]
    fn test_export_format_extension() {
        assert_eq!(ExportFormat::Csv.extension(), "csv");
        assert_eq!(ExportFormat::Json.extension(), "json");
        assert_eq!(ExportFormat::PrettyJson.extension(), "json");
    }

    #[test]
    fn test_multiple_risk_decompositions() {
        let mut contrib1 = HashMap::new();
        contrib1.insert("momentum".to_string(), 0.15);

        let mut contrib2 = HashMap::new();
        contrib2.insert("value".to_string(), 0.12);

        let risks = vec![
            RiskDecompositionExport::new("AAPL".to_string(), 0.25, 0.20, 0.05, contrib1),
            RiskDecompositionExport::new("MSFT".to_string(), 0.22, 0.18, 0.04, contrib2),
        ];

        let csv = risks.export_to_string(ExportFormat::Csv).unwrap();
        assert!(csv.contains("AAPL"));
        assert!(csv.contains("MSFT"));

        let json = risks.export_to_string(ExportFormat::Json).unwrap();
        assert!(json.contains("\"AAPL\""));
        assert!(json.contains("\"MSFT\""));
    }

    #[test]
    fn test_portfolio_holding_creation() {
        let holding = PortfolioHolding::new("AAPL".to_string(), 0.25, Some(25000.0), Some(100.0));

        assert_eq!(holding.symbol, "AAPL");
        assert_eq!(holding.weight, 0.25);
        assert_eq!(holding.market_value, Some(25000.0));
        assert_eq!(holding.shares, Some(100.0));
    }

    #[test]
    fn test_factor_exposure_creation() {
        let exposure = FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.75,
            1.5,
        );

        assert_eq!(exposure.symbol, "AAPL");
        assert_eq!(exposure.factor_name, "momentum");
        assert_eq!(exposure.exposure, 0.75);
        assert_eq!(exposure.z_score, 1.5);
    }

    #[test]
    fn test_risk_decomposition_creation() {
        let mut contributions = HashMap::new();
        contributions.insert("momentum".to_string(), 0.15);

        let risk = RiskDecompositionExport::new(
            "AAPL".to_string(),
            0.25,
            0.20,
            0.05,
            contributions.clone(),
        );

        assert_eq!(risk.symbol, "AAPL");
        assert_eq!(risk.total_risk, 0.25);
        assert_eq!(risk.factor_risk, 0.20);
        assert_eq!(risk.specific_risk, 0.05);
        assert_eq!(risk.factor_contributions, contributions);
    }
}
