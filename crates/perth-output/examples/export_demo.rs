//! Demonstration of the export functionality in perth-output.

use chrono::NaiveDate;
use perth_output::{
    ExportFormat, Exporter, FactorExposureExport, PortfolioExport, PortfolioHolding,
    RiskDecompositionExport,
};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Perth Export Demo ===\n");

    // 1. Factor Exposure Export Example
    println!("1. Factor Exposure Export\n");

    let exposures = vec![
        FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.75,
            1.5,
        ),
        FactorExposureExport::new(
            "AAPL".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "value".to_string(),
            -0.3,
            -0.6,
        ),
        FactorExposureExport::new(
            "MSFT".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            "momentum".to_string(),
            0.5,
            1.0,
        ),
    ];

    println!("CSV Format:");
    println!("{}\n", exposures.export_to_string(ExportFormat::Csv)?);

    println!("Pretty JSON Format:");
    println!(
        "{}\n",
        exposures.export_to_string(ExportFormat::PrettyJson)?
    );

    // 2. Risk Decomposition Export Example
    println!("\n2. Risk Decomposition Export\n");

    let mut contributions = HashMap::new();
    contributions.insert("momentum".to_string(), 0.15);
    contributions.insert("value".to_string(), 0.08);
    contributions.insert("size".to_string(), 0.05);

    let risk = RiskDecompositionExport::new("AAPL".to_string(), 0.28, 0.23, 0.05, contributions);

    println!("CSV Format:");
    println!("{}\n", risk.export_to_string(ExportFormat::Csv)?);

    println!("Pretty JSON Format:");
    println!("{}\n", risk.export_to_string(ExportFormat::PrettyJson)?);

    // 3. Portfolio Export Example
    println!("\n3. Portfolio Export\n");

    let holdings = vec![
        PortfolioHolding::new("AAPL".to_string(), 0.35, Some(350000.0), Some(1000.0)),
        PortfolioHolding::new("MSFT".to_string(), 0.30, Some(300000.0), Some(750.0)),
        PortfolioHolding::new("GOOGL".to_string(), 0.25, Some(250000.0), Some(500.0)),
        PortfolioHolding::new("AMZN".to_string(), 0.10, Some(100000.0), Some(200.0)),
    ];

    let portfolio = PortfolioExport::new(
        "Tech Growth Portfolio".to_string(),
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        holdings,
    );

    println!("CSV Format:");
    println!("{}\n", portfolio.export_to_string(ExportFormat::Csv)?);

    println!("Pretty JSON Format:");
    println!(
        "{}\n",
        portfolio.export_to_string(ExportFormat::PrettyJson)?
    );

    // 4. Export to File Example
    println!("\n4. Export to File Example\n");

    let temp_dir = std::env::temp_dir();
    let csv_file = temp_dir.join("portfolio_export.csv");
    let json_file = temp_dir.join("portfolio_export.json");

    portfolio.export_to_file(&csv_file, ExportFormat::Csv)?;
    portfolio.export_to_file(&json_file, ExportFormat::PrettyJson)?;

    println!("Exported portfolio to:");
    println!("  CSV: {}", csv_file.display());
    println!("  JSON: {}", json_file.display());

    Ok(())
}
