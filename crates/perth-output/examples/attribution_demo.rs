//! Demonstration of factor attribution and risk summary functionality.

use chrono::NaiveDate;
use perth_output::{
    FactorAttribution, PortfolioAttribution, SecurityAttribution, generate_risk_summary,
};
use std::collections::HashMap;

fn main() {
    println!("Perth Factor Attribution Demo\n");

    // Define the analysis period
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    // Create factor attributions for AAPL
    let aapl_factors = vec![
        FactorAttribution::new("Market".to_string(), 1.15, 0.12, 0.20),
        FactorAttribution::new("Technology".to_string(), 0.85, 0.08, 0.20),
        FactorAttribution::new("Growth".to_string(), 0.45, 0.05, 0.20),
    ];

    let aapl_attribution = SecurityAttribution::new(
        "AAPL".to_string(),
        start,
        end,
        0.20, // 20% total return
        aapl_factors,
    );

    // Create factor attributions for MSFT
    let msft_factors = vec![
        FactorAttribution::new("Market".to_string(), 1.05, 0.12, 0.18),
        FactorAttribution::new("Technology".to_string(), 0.75, 0.08, 0.18),
        FactorAttribution::new("Growth".to_string(), 0.35, 0.05, 0.18),
    ];

    let msft_attribution = SecurityAttribution::new(
        "MSFT".to_string(),
        start,
        end,
        0.18, // 18% total return
        msft_factors,
    );

    // Display individual security attributions
    println!("{}", aapl_attribution.to_ascii_table());
    println!("\n{}", msft_attribution.to_ascii_table());

    // Create portfolio attribution
    let portfolio = PortfolioAttribution::new_weighted(
        "Tech Leaders Portfolio".to_string(),
        vec![aapl_attribution, msft_attribution],
        vec![0.6, 0.4], // 60% AAPL, 40% MSFT
    );

    println!("\n{}", portfolio.to_ascii_table());

    // Generate risk summary
    println!("\n{}\n", "=".repeat(80));
    println!("Risk Analysis\n");

    let mut exposures = HashMap::new();
    exposures.insert("Market".to_string(), 1.11); // Weighted average: 0.6*1.15 + 0.4*1.05
    exposures.insert("Technology".to_string(), 0.81);
    exposures.insert("Growth".to_string(), 0.41);

    let mut volatilities = HashMap::new();
    volatilities.insert("Market".to_string(), 0.15);
    volatilities.insert("Technology".to_string(), 0.20);
    volatilities.insert("Growth".to_string(), 0.12);

    let mut risk_summary = generate_risk_summary(
        "Tech Leaders Portfolio".to_string(),
        start,
        end,
        exposures,
        volatilities,
        0.08, // Specific volatility
    );

    // Set portfolio value for monetary VaR calculation
    risk_summary.set_portfolio_value(1_000_000.0);

    println!("{}", risk_summary.to_ascii_table());

    // Export as Markdown
    println!("\n{}\n", "=".repeat(80));
    println!("Markdown Export Sample:\n");
    println!("{}", portfolio.to_markdown());
}
