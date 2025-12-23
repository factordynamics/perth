//! Factor attribution analysis for portfolio returns.
//!
//! This module provides structures and utilities for decomposing security and portfolio
//! returns into factor contributions and specific returns.

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Factor attribution for a single factor.
///
/// Represents the contribution of a single factor to a security's return,
/// calculated as the product of the factor exposure and the factor return.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FactorAttribution {
    /// Name of the factor (e.g., "Market", "Size", "Value").
    pub factor_name: String,

    /// The security's exposure to this factor (loading or beta).
    pub exposure: f64,

    /// The factor's return during the analysis period.
    pub factor_return: f64,

    /// The factor's contribution to total return (exposure * factor_return).
    pub contribution: f64,

    /// The factor's contribution as a percentage of total return.
    pub contribution_pct: f64,
}

impl FactorAttribution {
    /// Create a new factor attribution.
    ///
    /// # Arguments
    ///
    /// * `factor_name` - Name of the factor
    /// * `exposure` - Security's exposure to the factor
    /// * `factor_return` - Factor's return during the period
    /// * `total_return` - Total security return for percentage calculation
    ///
    /// # Examples
    ///
    /// ```
    /// use perth_output::FactorAttribution;
    ///
    /// let attribution = FactorAttribution::new(
    ///     "Market".to_string(),
    ///     1.2,      // 1.2x market exposure
    ///     0.10,     // 10% market return
    ///     0.15,     // 15% total return
    /// );
    ///
    /// assert_eq!(attribution.contribution, 0.12); // 1.2 * 0.10
    /// ```
    pub fn new(factor_name: String, exposure: f64, factor_return: f64, total_return: f64) -> Self {
        let contribution = exposure * factor_return;
        let contribution_pct = if total_return.abs() > 1e-10 {
            (contribution / total_return) * 100.0
        } else {
            0.0
        };

        Self {
            factor_name,
            exposure,
            factor_return,
            contribution,
            contribution_pct,
        }
    }
}

impl fmt::Display for FactorAttribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {:.2}% (exposure: {:.3}, return: {:.2}%)",
            self.factor_name,
            self.contribution * 100.0,
            self.exposure,
            self.factor_return * 100.0
        )
    }
}

/// Security-level attribution analysis.
///
/// Decomposes a security's return over a period into factor contributions
/// and specific (idiosyncratic) return.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityAttribution {
    /// Security symbol or identifier.
    pub symbol: String,

    /// Start date of the analysis period.
    pub period_start: NaiveDate,

    /// End date of the analysis period.
    pub period_end: NaiveDate,

    /// Total return over the period.
    pub total_return: f64,

    /// Sum of all factor contributions.
    pub factor_return: f64,

    /// Residual return not explained by factors.
    pub specific_return: f64,

    /// Individual factor attributions.
    pub factors: Vec<FactorAttribution>,
}

impl SecurityAttribution {
    /// Create a new security attribution.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Security identifier
    /// * `period_start` - Start date of analysis
    /// * `period_end` - End date of analysis
    /// * `total_return` - Total security return
    /// * `factors` - Vector of factor attributions
    ///
    /// # Examples
    ///
    /// ```
    /// use perth_output::{SecurityAttribution, FactorAttribution};
    /// use chrono::NaiveDate;
    ///
    /// let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
    ///
    /// let factors = vec![
    ///     FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15),
    ///     FactorAttribution::new("Size".to_string(), 0.5, 0.05, 0.15),
    /// ];
    ///
    /// let attribution = SecurityAttribution::new(
    ///     "AAPL".to_string(),
    ///     start,
    ///     end,
    ///     0.15,
    ///     factors,
    /// );
    ///
    /// assert_eq!(attribution.symbol, "AAPL");
    /// assert!(attribution.specific_return.abs() < 0.01);
    /// ```
    pub fn new(
        symbol: String,
        period_start: NaiveDate,
        period_end: NaiveDate,
        total_return: f64,
        factors: Vec<FactorAttribution>,
    ) -> Self {
        let factor_return: f64 = factors.iter().map(|f| f.contribution).sum();
        let specific_return = total_return - factor_return;

        Self {
            symbol,
            period_start,
            period_end,
            total_return,
            factor_return,
            specific_return,
            factors,
        }
    }

    /// Get the R-squared (proportion of variance explained by factors).
    ///
    /// # Returns
    ///
    /// Value between 0.0 and 1.0 indicating the proportion of total return
    /// explained by factors.
    pub fn r_squared(&self) -> f64 {
        if self.total_return.abs() < 1e-10 {
            return 0.0;
        }
        (self.factor_return / self.total_return)
            .powi(2)
            .clamp(0.0, 1.0)
    }

    /// Format as ASCII table for terminal display.
    pub fn to_ascii_table(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&format!("\nFactor Attribution: {}\n", self.symbol));
        output.push_str(&format!(
            "Period: {} to {}\n",
            self.period_start, self.period_end
        ));
        output.push_str(&"=".repeat(80));
        output.push('\n');

        // Table header
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>12} {:>12}\n",
            "Factor", "Exposure", "Return", "Contribution", "% of Total"
        ));
        output.push_str(&"-".repeat(80));
        output.push('\n');

        // Factor rows
        for factor in &self.factors {
            output.push_str(&format!(
                "{:<20} {:>12.4} {:>11.2}% {:>11.2}% {:>11.2}%\n",
                factor.factor_name,
                factor.exposure,
                factor.factor_return * 100.0,
                factor.contribution * 100.0,
                factor.contribution_pct
            ));
        }

        output.push_str(&"-".repeat(80));
        output.push('\n');

        // Summary
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>11.2}%\n",
            "Factor Return",
            "",
            "",
            self.factor_return * 100.0
        ));
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>11.2}%\n",
            "Specific Return",
            "",
            "",
            self.specific_return * 100.0
        ));
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>11.2}%\n",
            "Total Return",
            "",
            "",
            self.total_return * 100.0
        ));
        output.push_str(&"=".repeat(80));
        output.push('\n');
        output.push_str(&format!("R-squared: {:.4}\n", self.r_squared()));

        output
    }

    /// Format as Markdown table for documentation.
    pub fn to_markdown(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&format!("# Factor Attribution: {}\n\n", self.symbol));
        output.push_str(&format!(
            "**Period:** {} to {}\n\n",
            self.period_start, self.period_end
        ));

        // Table
        output.push_str("| Factor | Exposure | Return | Contribution | % of Total |\n");
        output.push_str("|--------|----------|--------|--------------|------------|\n");

        for factor in &self.factors {
            output.push_str(&format!(
                "| {} | {:.4} | {:.2}% | {:.2}% | {:.2}% |\n",
                factor.factor_name,
                factor.exposure,
                factor.factor_return * 100.0,
                factor.contribution * 100.0,
                factor.contribution_pct
            ));
        }

        output.push('\n');

        // Summary
        output.push_str("## Summary\n\n");
        output.push_str(&format!(
            "- **Factor Return:** {:.2}%\n",
            self.factor_return * 100.0
        ));
        output.push_str(&format!(
            "- **Specific Return:** {:.2}%\n",
            self.specific_return * 100.0
        ));
        output.push_str(&format!(
            "- **Total Return:** {:.2}%\n",
            self.total_return * 100.0
        ));
        output.push_str(&format!("- **R-squared:** {:.4}\n", self.r_squared()));

        output
    }
}

impl fmt::Display for SecurityAttribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Attribution for {} ({} to {}):",
            self.symbol, self.period_start, self.period_end
        )?;
        writeln!(f, "  Total Return: {:.2}%", self.total_return * 100.0)?;
        writeln!(f, "  Factor Return: {:.2}%", self.factor_return * 100.0)?;
        writeln!(f, "  Specific Return: {:.2}%", self.specific_return * 100.0)?;
        writeln!(f, "  R-squared: {:.4}", self.r_squared())?;
        writeln!(f, "  Factors:")?;
        for factor in &self.factors {
            writeln!(f, "    {}", factor)?;
        }
        Ok(())
    }
}

/// Portfolio-level attribution analysis.
///
/// Aggregates attribution across multiple securities to show factor
/// contributions at the portfolio level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortfolioAttribution {
    /// Portfolio name or identifier.
    pub portfolio_name: String,

    /// Start date of the analysis period.
    pub period_start: NaiveDate,

    /// End date of the analysis period.
    pub period_end: NaiveDate,

    /// Portfolio total return (weighted average).
    pub total_return: f64,

    /// Sum of all factor contributions.
    pub factor_return: f64,

    /// Portfolio-level specific return.
    pub specific_return: f64,

    /// Aggregated factor attributions.
    pub factors: Vec<FactorAttribution>,

    /// Individual security attributions.
    pub securities: Vec<SecurityAttribution>,
}

impl PortfolioAttribution {
    /// Create a new portfolio attribution from security attributions.
    ///
    /// # Arguments
    ///
    /// * `portfolio_name` - Portfolio identifier
    /// * `securities` - Vector of security attributions with equal weights
    ///
    /// # Examples
    ///
    /// ```
    /// use perth_output::{PortfolioAttribution, SecurityAttribution, FactorAttribution};
    /// use chrono::NaiveDate;
    ///
    /// let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
    ///
    /// let sec1 = SecurityAttribution::new(
    ///     "AAPL".to_string(),
    ///     start,
    ///     end,
    ///     0.15,
    ///     vec![FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15)],
    /// );
    ///
    /// let portfolio = PortfolioAttribution::new(
    ///     "Tech Portfolio".to_string(),
    ///     vec![sec1],
    /// );
    ///
    /// assert_eq!(portfolio.securities.len(), 1);
    /// ```
    pub fn new(portfolio_name: String, securities: Vec<SecurityAttribution>) -> Self {
        if securities.is_empty() {
            return Self {
                portfolio_name,
                period_start: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                period_end: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
                total_return: 0.0,
                factor_return: 0.0,
                specific_return: 0.0,
                factors: Vec::new(),
                securities: Vec::new(),
            };
        }

        let period_start = securities[0].period_start;
        let period_end = securities[0].period_end;
        let n = securities.len() as f64;

        // Equal-weighted portfolio returns
        let total_return = securities.iter().map(|s| s.total_return).sum::<f64>() / n;
        let factor_return = securities.iter().map(|s| s.factor_return).sum::<f64>() / n;
        let specific_return = securities.iter().map(|s| s.specific_return).sum::<f64>() / n;

        // Aggregate factors across securities
        let mut factor_map: std::collections::HashMap<String, (f64, f64, f64)> =
            std::collections::HashMap::new();

        for sec in &securities {
            for factor in &sec.factors {
                let entry = factor_map
                    .entry(factor.factor_name.clone())
                    .or_insert((0.0, 0.0, 0.0));
                entry.0 += factor.exposure / n;
                entry.1 += factor.factor_return / n;
                entry.2 += factor.contribution / n;
            }
        }

        let mut factors: Vec<FactorAttribution> = factor_map
            .into_iter()
            .map(|(name, (exposure, return_val, contribution))| {
                let contribution_pct = if total_return.abs() > 1e-10 {
                    (contribution / total_return) * 100.0
                } else {
                    0.0
                };
                FactorAttribution {
                    factor_name: name,
                    exposure,
                    factor_return: return_val,
                    contribution,
                    contribution_pct,
                }
            })
            .collect();

        // Sort by absolute contribution
        factors.sort_by(|a, b| {
            b.contribution
                .abs()
                .partial_cmp(&a.contribution.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Self {
            portfolio_name,
            period_start,
            period_end,
            total_return,
            factor_return,
            specific_return,
            factors,
            securities,
        }
    }

    /// Create a portfolio attribution with custom weights.
    ///
    /// # Arguments
    ///
    /// * `portfolio_name` - Portfolio identifier
    /// * `securities` - Vector of security attributions
    /// * `weights` - Vector of weights (must sum to 1.0)
    ///
    /// # Panics
    ///
    /// Panics if weights and securities have different lengths or weights don't sum to ~1.0.
    pub fn new_weighted(
        portfolio_name: String,
        securities: Vec<SecurityAttribution>,
        weights: Vec<f64>,
    ) -> Self {
        assert_eq!(
            securities.len(),
            weights.len(),
            "Securities and weights must have same length"
        );
        assert!(
            (weights.iter().sum::<f64>() - 1.0).abs() < 1e-6,
            "Weights must sum to 1.0"
        );

        if securities.is_empty() {
            return Self::new(portfolio_name, securities);
        }

        let period_start = securities[0].period_start;
        let period_end = securities[0].period_end;

        // Weighted portfolio returns
        let total_return: f64 = securities
            .iter()
            .zip(&weights)
            .map(|(s, w)| s.total_return * w)
            .sum();
        let factor_return: f64 = securities
            .iter()
            .zip(&weights)
            .map(|(s, w)| s.factor_return * w)
            .sum();
        let specific_return: f64 = securities
            .iter()
            .zip(&weights)
            .map(|(s, w)| s.specific_return * w)
            .sum();

        // Aggregate factors with weights
        let mut factor_map: std::collections::HashMap<String, (f64, f64, f64)> =
            std::collections::HashMap::new();

        for (sec, weight) in securities.iter().zip(&weights) {
            for factor in &sec.factors {
                let entry = factor_map
                    .entry(factor.factor_name.clone())
                    .or_insert((0.0, 0.0, 0.0));
                entry.0 += factor.exposure * weight;
                entry.1 += factor.factor_return * weight;
                entry.2 += factor.contribution * weight;
            }
        }

        let mut factors: Vec<FactorAttribution> = factor_map
            .into_iter()
            .map(|(name, (exposure, return_val, contribution))| {
                let contribution_pct = if total_return.abs() > 1e-10 {
                    (contribution / total_return) * 100.0
                } else {
                    0.0
                };
                FactorAttribution {
                    factor_name: name,
                    exposure,
                    factor_return: return_val,
                    contribution,
                    contribution_pct,
                }
            })
            .collect();

        factors.sort_by(|a, b| {
            b.contribution
                .abs()
                .partial_cmp(&a.contribution.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Self {
            portfolio_name,
            period_start,
            period_end,
            total_return,
            factor_return,
            specific_return,
            factors,
            securities,
        }
    }

    /// Get the portfolio R-squared.
    pub fn r_squared(&self) -> f64 {
        if self.total_return.abs() < 1e-10 {
            return 0.0;
        }
        (self.factor_return / self.total_return)
            .powi(2)
            .clamp(0.0, 1.0)
    }

    /// Format as ASCII table for terminal display.
    pub fn to_ascii_table(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!(
            "\nPortfolio Factor Attribution: {}\n",
            self.portfolio_name
        ));
        output.push_str(&format!(
            "Period: {} to {}\n",
            self.period_start, self.period_end
        ));
        output.push_str(&format!(
            "Number of Securities: {}\n",
            self.securities.len()
        ));
        output.push_str(&"=".repeat(80));
        output.push('\n');

        // Portfolio-level factors
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>12} {:>12}\n",
            "Factor", "Exposure", "Return", "Contribution", "% of Total"
        ));
        output.push_str(&"-".repeat(80));
        output.push('\n');

        for factor in &self.factors {
            output.push_str(&format!(
                "{:<20} {:>12.4} {:>11.2}% {:>11.2}% {:>11.2}%\n",
                factor.factor_name,
                factor.exposure,
                factor.factor_return * 100.0,
                factor.contribution * 100.0,
                factor.contribution_pct
            ));
        }

        output.push_str(&"-".repeat(80));
        output.push('\n');
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>11.2}%\n",
            "Factor Return",
            "",
            "",
            self.factor_return * 100.0
        ));
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>11.2}%\n",
            "Specific Return",
            "",
            "",
            self.specific_return * 100.0
        ));
        output.push_str(&format!(
            "{:<20} {:>12} {:>12} {:>11.2}%\n",
            "Total Return",
            "",
            "",
            self.total_return * 100.0
        ));
        output.push_str(&"=".repeat(80));
        output.push('\n');
        output.push_str(&format!("Portfolio R-squared: {:.4}\n\n", self.r_squared()));

        // Individual securities summary
        output.push_str("Individual Securities:\n");
        output.push_str(&"-".repeat(80));
        output.push('\n');
        output.push_str(&format!(
            "{:<10} {:>15} {:>15} {:>15} {:>15}\n",
            "Symbol", "Total Return", "Factor Return", "Specific Return", "R-squared"
        ));
        output.push_str(&"-".repeat(80));
        output.push('\n');

        for sec in &self.securities {
            output.push_str(&format!(
                "{:<10} {:>14.2}% {:>14.2}% {:>14.2}% {:>15.4}\n",
                sec.symbol,
                sec.total_return * 100.0,
                sec.factor_return * 100.0,
                sec.specific_return * 100.0,
                sec.r_squared()
            ));
        }

        output
    }

    /// Format as Markdown for documentation.
    pub fn to_markdown(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!(
            "# Portfolio Factor Attribution: {}\n\n",
            self.portfolio_name
        ));
        output.push_str(&format!(
            "**Period:** {} to {}\n\n",
            self.period_start, self.period_end
        ));
        output.push_str(&format!(
            "**Number of Securities:** {}\n\n",
            self.securities.len()
        ));

        output.push_str("## Portfolio-Level Attribution\n\n");
        output.push_str("| Factor | Exposure | Return | Contribution | % of Total |\n");
        output.push_str("|--------|----------|--------|--------------|------------|\n");

        for factor in &self.factors {
            output.push_str(&format!(
                "| {} | {:.4} | {:.2}% | {:.2}% | {:.2}% |\n",
                factor.factor_name,
                factor.exposure,
                factor.factor_return * 100.0,
                factor.contribution * 100.0,
                factor.contribution_pct
            ));
        }

        output.push_str("\n### Summary\n\n");
        output.push_str(&format!(
            "- **Factor Return:** {:.2}%\n",
            self.factor_return * 100.0
        ));
        output.push_str(&format!(
            "- **Specific Return:** {:.2}%\n",
            self.specific_return * 100.0
        ));
        output.push_str(&format!(
            "- **Total Return:** {:.2}%\n",
            self.total_return * 100.0
        ));
        output.push_str(&format!(
            "- **Portfolio R-squared:** {:.4}\n\n",
            self.r_squared()
        ));

        output.push_str("## Individual Securities\n\n");
        output
            .push_str("| Symbol | Total Return | Factor Return | Specific Return | R-squared |\n");
        output.push_str("|--------|--------------|---------------|-----------------|----------|\n");

        for sec in &self.securities {
            output.push_str(&format!(
                "| {} | {:.2}% | {:.2}% | {:.2}% | {:.4} |\n",
                sec.symbol,
                sec.total_return * 100.0,
                sec.factor_return * 100.0,
                sec.specific_return * 100.0,
                sec.r_squared()
            ));
        }

        output
    }
}

impl fmt::Display for PortfolioAttribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Portfolio Attribution: {} ({} to {})",
            self.portfolio_name, self.period_start, self.period_end
        )?;
        writeln!(f, "  Total Return: {:.2}%", self.total_return * 100.0)?;
        writeln!(f, "  Factor Return: {:.2}%", self.factor_return * 100.0)?;
        writeln!(f, "  Specific Return: {:.2}%", self.specific_return * 100.0)?;
        writeln!(f, "  R-squared: {:.4}", self.r_squared())?;
        writeln!(f, "  Securities: {}", self.securities.len())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_attribution_creation() {
        let attr = FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15);

        assert_eq!(attr.factor_name, "Market");
        assert_eq!(attr.exposure, 1.2);
        assert_eq!(attr.factor_return, 0.10);
        assert_eq!(attr.contribution, 0.12);
        assert!((attr.contribution_pct - 80.0).abs() < 1e-6);
    }

    #[test]
    fn test_security_attribution() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let factors = vec![
            FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15),
            FactorAttribution::new("Size".to_string(), 0.5, 0.05, 0.15),
        ];

        let attr = SecurityAttribution::new("AAPL".to_string(), start, end, 0.15, factors);

        assert_eq!(attr.symbol, "AAPL");
        assert!((attr.factor_return - 0.145).abs() < 1e-6);
        assert!((attr.specific_return - 0.005).abs() < 1e-6);
        assert!(attr.r_squared() > 0.9);
    }

    #[test]
    fn test_security_attribution_ascii_table() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let factors = vec![FactorAttribution::new(
            "Market".to_string(),
            1.2,
            0.10,
            0.15,
        )];

        let attr = SecurityAttribution::new("AAPL".to_string(), start, end, 0.15, factors);

        let table = attr.to_ascii_table();
        assert!(table.contains("AAPL"));
        assert!(table.contains("Market"));
    }

    #[test]
    fn test_security_attribution_markdown() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let factors = vec![FactorAttribution::new(
            "Market".to_string(),
            1.2,
            0.10,
            0.15,
        )];

        let attr = SecurityAttribution::new("AAPL".to_string(), start, end, 0.15, factors);

        let md = attr.to_markdown();
        assert!(md.contains("# Factor Attribution"));
        assert!(md.contains("| Market |"));
    }

    #[test]
    fn test_portfolio_attribution_equal_weight() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let sec1 = SecurityAttribution::new(
            "AAPL".to_string(),
            start,
            end,
            0.15,
            vec![FactorAttribution::new(
                "Market".to_string(),
                1.2,
                0.10,
                0.15,
            )],
        );

        let sec2 = SecurityAttribution::new(
            "MSFT".to_string(),
            start,
            end,
            0.20,
            vec![FactorAttribution::new(
                "Market".to_string(),
                1.0,
                0.10,
                0.20,
            )],
        );

        let portfolio = PortfolioAttribution::new("Tech Portfolio".to_string(), vec![sec1, sec2]);

        assert_eq!(portfolio.portfolio_name, "Tech Portfolio");
        assert_eq!(portfolio.securities.len(), 2);
        assert!((portfolio.total_return - 0.175).abs() < 1e-6);
    }

    #[test]
    fn test_portfolio_attribution_weighted() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let sec1 = SecurityAttribution::new(
            "AAPL".to_string(),
            start,
            end,
            0.15,
            vec![FactorAttribution::new(
                "Market".to_string(),
                1.2,
                0.10,
                0.15,
            )],
        );

        let sec2 = SecurityAttribution::new(
            "MSFT".to_string(),
            start,
            end,
            0.20,
            vec![FactorAttribution::new(
                "Market".to_string(),
                1.0,
                0.10,
                0.20,
            )],
        );

        let portfolio = PortfolioAttribution::new_weighted(
            "Tech Portfolio".to_string(),
            vec![sec1, sec2],
            vec![0.6, 0.4],
        );

        // 0.6 * 0.15 + 0.4 * 0.20 = 0.17
        assert!((portfolio.total_return - 0.17).abs() < 1e-6);
    }

    #[test]
    #[should_panic(expected = "Weights must sum to 1.0")]
    fn test_portfolio_attribution_invalid_weights() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let sec1 = SecurityAttribution::new("AAPL".to_string(), start, end, 0.15, vec![]);

        PortfolioAttribution::new_weighted(
            "Portfolio".to_string(),
            vec![sec1],
            vec![0.5], // Doesn't sum to 1.0
        );
    }

    #[test]
    fn test_portfolio_ascii_table() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let sec1 = SecurityAttribution::new(
            "AAPL".to_string(),
            start,
            end,
            0.15,
            vec![FactorAttribution::new(
                "Market".to_string(),
                1.2,
                0.10,
                0.15,
            )],
        );

        let portfolio = PortfolioAttribution::new("Tech Portfolio".to_string(), vec![sec1]);

        let table = portfolio.to_ascii_table();
        assert!(table.contains("Tech Portfolio"));
        assert!(table.contains("AAPL"));
    }

    #[test]
    fn test_portfolio_markdown() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let sec1 = SecurityAttribution::new(
            "AAPL".to_string(),
            start,
            end,
            0.15,
            vec![FactorAttribution::new(
                "Market".to_string(),
                1.2,
                0.10,
                0.15,
            )],
        );

        let portfolio = PortfolioAttribution::new("Tech Portfolio".to_string(), vec![sec1]);

        let md = portfolio.to_markdown();
        assert!(md.contains("# Portfolio Factor Attribution"));
        assert!(md.contains("## Individual Securities"));
    }
}
