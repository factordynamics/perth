//! Integration tests for factor attribution and risk summary.

use chrono::NaiveDate;
use perth_output::{
    FactorAttribution, PortfolioAttribution, SecurityAttribution, generate_risk_summary,
};
use std::collections::HashMap;

#[test]
fn test_full_attribution_workflow() {
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    // Create security attributions
    let aapl_factors = vec![
        FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15),
        FactorAttribution::new("Technology".to_string(), 0.8, 0.05, 0.15),
    ];
    let aapl = SecurityAttribution::new("AAPL".to_string(), start, end, 0.15, aapl_factors);

    let msft_factors = vec![
        FactorAttribution::new("Market".to_string(), 1.0, 0.10, 0.12),
        FactorAttribution::new("Technology".to_string(), 0.6, 0.05, 0.12),
    ];
    let msft = SecurityAttribution::new("MSFT".to_string(), start, end, 0.12, msft_factors);

    // Create portfolio
    let portfolio = PortfolioAttribution::new_weighted(
        "Test Portfolio".to_string(),
        vec![aapl, msft],
        vec![0.5, 0.5],
    );

    // Verify portfolio calculations
    assert_eq!(portfolio.securities.len(), 2);
    assert!((portfolio.total_return - 0.135).abs() < 1e-6); // (0.15 + 0.12) / 2

    // Verify ASCII table generation doesn't panic
    let ascii = portfolio.to_ascii_table();
    assert!(ascii.contains("Test Portfolio"));
    assert!(ascii.contains("AAPL"));
    assert!(ascii.contains("MSFT"));

    // Verify Markdown generation doesn't panic
    let markdown = portfolio.to_markdown();
    assert!(markdown.contains("# Portfolio Factor Attribution"));
    assert!(markdown.contains("| Symbol |"));
}

#[test]
fn test_full_risk_workflow() {
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    let mut exposures = HashMap::new();
    exposures.insert("Market".to_string(), 1.0);
    exposures.insert("Size".to_string(), 0.5);
    exposures.insert("Value".to_string(), -0.2);

    let mut volatilities = HashMap::new();
    volatilities.insert("Market".to_string(), 0.15);
    volatilities.insert("Size".to_string(), 0.10);
    volatilities.insert("Value".to_string(), 0.12);

    let mut summary = generate_risk_summary(
        "Test Portfolio".to_string(),
        start,
        end,
        exposures,
        volatilities,
        0.05,
    );

    // Verify calculations
    assert!(summary.total_risk > 0.0);
    assert!(summary.factor_risk > 0.0);
    assert_eq!(summary.specific_risk, 0.05);
    assert_eq!(summary.factor_contributions.len(), 3);

    // Set portfolio value
    summary.set_portfolio_value(1_000_000.0);
    assert!(summary.var_95_monetary().is_some());
    assert!(summary.var_99_monetary().is_some());

    // Verify formatting
    let ascii = summary.to_ascii_table();
    assert!(ascii.contains("Test Portfolio"));
    assert!(ascii.contains("Total Risk"));
    assert!(ascii.contains("95% VaR"));
    assert!(ascii.contains("$")); // Should show monetary VaR

    let markdown = summary.to_markdown();
    assert!(markdown.contains("# Risk Summary"));
    assert!(markdown.contains("## Factor Risk Contributions"));
}

#[test]
fn test_r_squared_calculation() {
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    // Perfect factor explanation (R² = 1.0)
    let perfect_factors = vec![FactorAttribution::new(
        "Market".to_string(),
        1.0,
        0.10,
        0.10,
    )];
    let perfect =
        SecurityAttribution::new("PERFECT".to_string(), start, end, 0.10, perfect_factors);
    assert!((perfect.r_squared() - 1.0).abs() < 1e-6);

    // Partial factor explanation
    let partial_factors = vec![FactorAttribution::new(
        "Market".to_string(),
        1.0,
        0.10,
        0.20,
    )];
    let partial =
        SecurityAttribution::new("PARTIAL".to_string(), start, end, 0.20, partial_factors);
    assert!(partial.r_squared() > 0.0);
    assert!(partial.r_squared() < 1.0);
}

#[test]
fn test_zero_return_handling() {
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    // Zero total return should not cause division by zero
    let factors = vec![FactorAttribution::new("Market".to_string(), 0.0, 0.0, 0.0)];
    let zero_return = SecurityAttribution::new("ZERO".to_string(), start, end, 0.0, factors);

    // Should not panic
    let r_squared = zero_return.r_squared();
    assert_eq!(r_squared, 0.0);

    // Formatting should also work
    let _ = zero_return.to_ascii_table();
    let _ = zero_return.to_markdown();
}

#[test]
fn test_negative_returns() {
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    // Negative returns should be handled correctly
    let factors = vec![FactorAttribution::new(
        "Market".to_string(),
        1.0,
        -0.10,
        -0.15,
    )];
    let negative = SecurityAttribution::new("NEGATIVE".to_string(), start, end, -0.15, factors);

    assert!(negative.total_return < 0.0);
    assert!(negative.factor_return < 0.0);

    // Formatting should work with negative values
    let ascii = negative.to_ascii_table();
    assert!(ascii.contains("-"));
}
