//! Risk summary and factor risk decomposition.
//!
//! This module provides structures for analyzing and reporting risk metrics,
//! including total risk, factor risk, specific risk, and Value at Risk (VaR).

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Factor contribution to portfolio risk.
///
/// Represents how much a single factor contributes to the total portfolio risk,
/// measured in terms of variance contribution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FactorRiskContribution {
    /// Name of the factor.
    pub factor_name: String,

    /// Factor exposure (loading).
    pub exposure: f64,

    /// Factor volatility (standard deviation).
    pub factor_volatility: f64,

    /// Marginal contribution to risk (MCR).
    pub marginal_contribution: f64,

    /// Contribution to total risk variance.
    pub risk_contribution: f64,

    /// Percentage of total risk.
    pub risk_contribution_pct: f64,
}

impl FactorRiskContribution {
    /// Create a new factor risk contribution.
    ///
    /// # Arguments
    ///
    /// * `factor_name` - Name of the factor
    /// * `exposure` - Portfolio exposure to the factor
    /// * `factor_volatility` - Factor's standard deviation
    /// * `marginal_contribution` - Marginal contribution to risk
    /// * `total_risk` - Total portfolio risk for percentage calculation
    ///
    /// # Examples
    ///
    /// ```
    /// use perth_output::FactorRiskContribution;
    ///
    /// let risk = FactorRiskContribution::new(
    ///     "Market".to_string(),
    ///     1.2,     // 1.2x market exposure
    ///     0.15,    // 15% market volatility
    ///     0.018,   // Marginal contribution
    ///     0.20,    // 20% total portfolio risk
    /// );
    ///
    /// assert_eq!(risk.factor_name, "Market");
    /// ```
    pub fn new(
        factor_name: String,
        exposure: f64,
        factor_volatility: f64,
        marginal_contribution: f64,
        total_risk: f64,
    ) -> Self {
        let risk_contribution = exposure * marginal_contribution;
        let risk_contribution_pct = if total_risk.abs() > 1e-10 {
            (risk_contribution / total_risk.powi(2)) * 100.0
        } else {
            0.0
        };

        Self {
            factor_name,
            exposure,
            factor_volatility,
            marginal_contribution,
            risk_contribution,
            risk_contribution_pct,
        }
    }
}

impl fmt::Display for FactorRiskContribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {:.2}% of risk (exposure: {:.3}, volatility: {:.2}%)",
            self.factor_name,
            self.risk_contribution_pct,
            self.exposure,
            self.factor_volatility * 100.0
        )
    }
}

/// Comprehensive risk summary for a portfolio or security.
///
/// Provides a complete breakdown of risk including total risk, factor risk,
/// specific (idiosyncratic) risk, and Value at Risk metrics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskSummary {
    /// Entity name (portfolio or security symbol).
    pub name: String,

    /// Start date of the analysis period.
    pub period_start: NaiveDate,

    /// End date of the analysis period.
    pub period_end: NaiveDate,

    /// Total portfolio risk (standard deviation).
    pub total_risk: f64,

    /// Risk from factor exposures (systematic risk).
    pub factor_risk: f64,

    /// Specific risk (idiosyncratic, diversifiable risk).
    pub specific_risk: f64,

    /// 95% Value at Risk (VaR).
    pub var_95: f64,

    /// 99% Value at Risk (VaR).
    pub var_99: f64,

    /// Individual factor risk contributions.
    pub factor_contributions: Vec<FactorRiskContribution>,

    /// Portfolio value for VaR calculations.
    pub portfolio_value: Option<f64>,
}

impl RiskSummary {
    /// Create a new risk summary.
    ///
    /// # Arguments
    ///
    /// * `name` - Portfolio or security name
    /// * `period_start` - Start date of analysis
    /// * `period_end` - End date of analysis
    /// * `total_risk` - Total risk (standard deviation)
    /// * `factor_risk` - Risk from factors
    /// * `specific_risk` - Idiosyncratic risk
    /// * `factor_contributions` - Individual factor risk contributions
    ///
    /// # Examples
    ///
    /// ```
    /// use perth_output::{RiskSummary, FactorRiskContribution};
    /// use chrono::NaiveDate;
    ///
    /// let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
    ///
    /// let factors = vec![
    ///     FactorRiskContribution::new("Market".to_string(), 1.2, 0.15, 0.018, 0.20),
    /// ];
    ///
    /// let summary = RiskSummary::new(
    ///     "Tech Portfolio".to_string(),
    ///     start,
    ///     end,
    ///     0.20,
    ///     0.18,
    ///     0.05,
    ///     factors,
    /// );
    ///
    /// assert_eq!(summary.name, "Tech Portfolio");
    /// assert_eq!(summary.total_risk, 0.20);
    /// ```
    pub fn new(
        name: String,
        period_start: NaiveDate,
        period_end: NaiveDate,
        total_risk: f64,
        factor_risk: f64,
        specific_risk: f64,
        factor_contributions: Vec<FactorRiskContribution>,
    ) -> Self {
        // Calculate VaR assuming normal distribution
        // 95% VaR = 1.645 * sigma
        // 99% VaR = 2.326 * sigma
        let var_95 = total_risk * 1.645;
        let var_99 = total_risk * 2.326;

        Self {
            name,
            period_start,
            period_end,
            total_risk,
            factor_risk,
            specific_risk,
            var_95,
            var_99,
            factor_contributions,
            portfolio_value: None,
        }
    }

    /// Set the portfolio value for monetary VaR calculations.
    ///
    /// # Arguments
    ///
    /// * `value` - Portfolio value in currency units
    ///
    /// # Examples
    ///
    /// ```
    /// use perth_output::RiskSummary;
    /// use chrono::NaiveDate;
    ///
    /// let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    /// let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
    ///
    /// let mut summary = RiskSummary::new(
    ///     "Portfolio".to_string(),
    ///     start,
    ///     end,
    ///     0.20,
    ///     0.18,
    ///     0.05,
    ///     vec![],
    /// );
    ///
    /// summary.set_portfolio_value(1_000_000.0);
    /// assert_eq!(summary.portfolio_value, Some(1_000_000.0));
    /// ```
    pub const fn set_portfolio_value(&mut self, value: f64) {
        self.portfolio_value = Some(value);
    }

    /// Get 95% VaR in monetary terms.
    pub fn var_95_monetary(&self) -> Option<f64> {
        self.portfolio_value.map(|v| v * self.var_95)
    }

    /// Get 99% VaR in monetary terms.
    pub fn var_99_monetary(&self) -> Option<f64> {
        self.portfolio_value.map(|v| v * self.var_99)
    }

    /// Calculate the proportion of risk explained by factors.
    pub fn factor_risk_ratio(&self) -> f64 {
        if self.total_risk.abs() < 1e-10 {
            return 0.0;
        }
        self.factor_risk / self.total_risk
    }

    /// Calculate the proportion of risk from specific sources.
    pub fn specific_risk_ratio(&self) -> f64 {
        if self.total_risk.abs() < 1e-10 {
            return 0.0;
        }
        self.specific_risk / self.total_risk
    }

    /// Format as ASCII table for terminal display.
    pub fn to_ascii_table(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!("\nRisk Summary: {}\n", self.name));
        output.push_str(&format!(
            "Period: {} to {}\n",
            self.period_start, self.period_end
        ));
        output.push_str(&"=".repeat(80));
        output.push('\n');

        // Overall risk metrics
        output.push_str("\nOverall Risk Metrics:\n");
        output.push_str(&"-".repeat(80));
        output.push('\n');
        output.push_str(&format!(
            "  Total Risk (σ):           {:.2}%\n",
            self.total_risk * 100.0
        ));
        output.push_str(&format!(
            "  Factor Risk:              {:.2}% ({:.1}% of total)\n",
            self.factor_risk * 100.0,
            self.factor_risk_ratio() * 100.0
        ));
        output.push_str(&format!(
            "  Specific Risk:            {:.2}% ({:.1}% of total)\n",
            self.specific_risk * 100.0,
            self.specific_risk_ratio() * 100.0
        ));
        output.push_str(&format!(
            "  95% VaR:                  {:.2}%",
            self.var_95 * 100.0
        ));
        if let Some(var_95_money) = self.var_95_monetary() {
            output.push_str(&format!(" (${:.2})", var_95_money));
        }
        output.push('\n');
        output.push_str(&format!(
            "  99% VaR:                  {:.2}%",
            self.var_99 * 100.0
        ));
        if let Some(var_99_money) = self.var_99_monetary() {
            output.push_str(&format!(" (${:.2})", var_99_money));
        }
        output.push('\n');

        // Factor risk decomposition
        if !self.factor_contributions.is_empty() {
            output.push_str("\nFactor Risk Contributions:\n");
            output.push_str(&"-".repeat(80));
            output.push('\n');
            output.push_str(&format!(
                "{:<20} {:>12} {:>12} {:>12} {:>12}\n",
                "Factor", "Exposure", "Volatility", "Risk Contr.", "% of Total"
            ));
            output.push_str(&"-".repeat(80));
            output.push('\n');

            for factor in &self.factor_contributions {
                output.push_str(&format!(
                    "{:<20} {:>12.4} {:>11.2}% {:>12.6} {:>11.2}%\n",
                    factor.factor_name,
                    factor.exposure,
                    factor.factor_volatility * 100.0,
                    factor.risk_contribution,
                    factor.risk_contribution_pct
                ));
            }
        }

        output.push_str(&"=".repeat(80));
        output.push('\n');

        output
    }

    /// Format as Markdown for documentation.
    pub fn to_markdown(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!("# Risk Summary: {}\n\n", self.name));
        output.push_str(&format!(
            "**Period:** {} to {}\n\n",
            self.period_start, self.period_end
        ));

        // Overall metrics
        output.push_str("## Overall Risk Metrics\n\n");
        output.push_str(&format!(
            "- **Total Risk (σ):** {:.2}%\n",
            self.total_risk * 100.0
        ));
        output.push_str(&format!(
            "- **Factor Risk:** {:.2}% ({:.1}% of total)\n",
            self.factor_risk * 100.0,
            self.factor_risk_ratio() * 100.0
        ));
        output.push_str(&format!(
            "- **Specific Risk:** {:.2}% ({:.1}% of total)\n",
            self.specific_risk * 100.0,
            self.specific_risk_ratio() * 100.0
        ));
        output.push_str(&format!("- **95% VaR:** {:.2}%", self.var_95 * 100.0));
        if let Some(var_95_money) = self.var_95_monetary() {
            output.push_str(&format!(" (${:.2})", var_95_money));
        }
        output.push('\n');
        output.push_str(&format!("- **99% VaR:** {:.2}%", self.var_99 * 100.0));
        if let Some(var_99_money) = self.var_99_monetary() {
            output.push_str(&format!(" (${:.2})", var_99_money));
        }
        output.push_str("\n\n");

        // Factor decomposition
        if !self.factor_contributions.is_empty() {
            output.push_str("## Factor Risk Contributions\n\n");
            output
                .push_str("| Factor | Exposure | Volatility | Risk Contribution | % of Total |\n");
            output
                .push_str("|--------|----------|------------|-------------------|------------|\n");

            for factor in &self.factor_contributions {
                output.push_str(&format!(
                    "| {} | {:.4} | {:.2}% | {:.6} | {:.2}% |\n",
                    factor.factor_name,
                    factor.exposure,
                    factor.factor_volatility * 100.0,
                    factor.risk_contribution,
                    factor.risk_contribution_pct
                ));
            }
        }

        output
    }
}

impl fmt::Display for RiskSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Risk Summary: {} ({} to {})",
            self.name, self.period_start, self.period_end
        )?;
        writeln!(f, "  Total Risk: {:.2}%", self.total_risk * 100.0)?;
        writeln!(f, "  Factor Risk: {:.2}%", self.factor_risk * 100.0)?;
        writeln!(f, "  Specific Risk: {:.2}%", self.specific_risk * 100.0)?;
        writeln!(f, "  95% VaR: {:.2}%", self.var_95 * 100.0)?;
        writeln!(f, "  99% VaR: {:.2}%", self.var_99 * 100.0)?;
        Ok(())
    }
}

/// Generate a risk summary from factor exposures and covariance matrix.
///
/// # Arguments
///
/// * `name` - Portfolio or security name
/// * `period_start` - Start date of analysis period
/// * `period_end` - End date of analysis period
/// * `exposures` - Map of factor names to exposures
/// * `factor_volatilities` - Map of factor names to volatilities
/// * `specific_volatility` - Idiosyncratic volatility
///
/// # Returns
///
/// A `RiskSummary` with calculated risk metrics.
///
/// # Examples
///
/// ```
/// use perth_output::generate_risk_summary;
/// use chrono::NaiveDate;
/// use std::collections::HashMap;
///
/// let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
/// let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
///
/// let mut exposures = HashMap::new();
/// exposures.insert("Market".to_string(), 1.2);
///
/// let mut volatilities = HashMap::new();
/// volatilities.insert("Market".to_string(), 0.15);
///
/// let summary = generate_risk_summary(
///     "Portfolio".to_string(),
///     start,
///     end,
///     exposures,
///     volatilities,
///     0.05,
/// );
///
/// assert!(summary.total_risk > 0.0);
/// ```
pub fn generate_risk_summary(
    name: String,
    period_start: NaiveDate,
    period_end: NaiveDate,
    exposures: std::collections::HashMap<String, f64>,
    factor_volatilities: std::collections::HashMap<String, f64>,
    specific_volatility: f64,
) -> RiskSummary {
    // Simple diagonal covariance assumption (factors are uncorrelated)
    // In practice, would use full covariance matrix

    let mut factor_variance = 0.0;
    let mut factor_contributions = Vec::new();

    for (factor_name, exposure) in &exposures {
        if let Some(&volatility) = factor_volatilities.get(factor_name) {
            let variance_contrib = exposure.powi(2) * volatility.powi(2);
            factor_variance += variance_contrib;

            // Marginal contribution to risk
            let mcr = exposure * volatility.powi(2);

            let contribution = FactorRiskContribution {
                factor_name: factor_name.clone(),
                exposure: *exposure,
                factor_volatility: volatility,
                marginal_contribution: mcr,
                risk_contribution: variance_contrib,
                risk_contribution_pct: 0.0, // Will be updated below
            };
            factor_contributions.push(contribution);
        }
    }

    let factor_risk = factor_variance.sqrt();
    let specific_variance = specific_volatility.powi(2);
    let total_variance = factor_variance + specific_variance;
    let total_risk = total_variance.sqrt();

    // Update percentage contributions
    for contrib in &mut factor_contributions {
        contrib.risk_contribution_pct = if total_variance > 1e-10 {
            (contrib.risk_contribution / total_variance) * 100.0
        } else {
            0.0
        };
    }

    // Sort by risk contribution (descending)
    factor_contributions.sort_by(|a, b| {
        b.risk_contribution
            .partial_cmp(&a.risk_contribution)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    RiskSummary::new(
        name,
        period_start,
        period_end,
        total_risk,
        factor_risk,
        specific_volatility,
        factor_contributions,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_factor_risk_contribution() {
        let risk = FactorRiskContribution::new("Market".to_string(), 1.2, 0.15, 0.018, 0.20);

        assert_eq!(risk.factor_name, "Market");
        assert_eq!(risk.exposure, 1.2);
        assert_eq!(risk.factor_volatility, 0.15);
        assert!((risk.risk_contribution - 0.0216).abs() < 1e-6);
    }

    #[test]
    fn test_risk_summary_creation() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let factors = vec![FactorRiskContribution::new(
            "Market".to_string(),
            1.2,
            0.15,
            0.018,
            0.20,
        )];

        let summary = RiskSummary::new(
            "Portfolio".to_string(),
            start,
            end,
            0.20,
            0.18,
            0.05,
            factors,
        );

        assert_eq!(summary.name, "Portfolio");
        assert_eq!(summary.total_risk, 0.20);
        assert_eq!(summary.factor_risk, 0.18);
        assert_eq!(summary.specific_risk, 0.05);
        assert!((summary.var_95 - 0.329).abs() < 1e-3);
        assert!((summary.var_99 - 0.4652).abs() < 1e-3);
    }

    #[test]
    fn test_risk_summary_with_portfolio_value() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let mut summary = RiskSummary::new(
            "Portfolio".to_string(),
            start,
            end,
            0.20,
            0.18,
            0.05,
            vec![],
        );

        summary.set_portfolio_value(1_000_000.0);

        assert_eq!(summary.portfolio_value, Some(1_000_000.0));
        assert!((summary.var_95_monetary().unwrap() - 329_000.0).abs() < 1.0);
        assert!((summary.var_99_monetary().unwrap() - 465_200.0).abs() < 1.0);
    }

    #[test]
    fn test_risk_ratios() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let summary = RiskSummary::new(
            "Portfolio".to_string(),
            start,
            end,
            0.20,
            0.18,
            0.05,
            vec![],
        );

        assert!((summary.factor_risk_ratio() - 0.9).abs() < 1e-6);
        assert!((summary.specific_risk_ratio() - 0.25).abs() < 1e-6);
    }

    #[test]
    fn test_generate_risk_summary() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let mut exposures = HashMap::new();
        exposures.insert("Market".to_string(), 1.0);
        exposures.insert("Size".to_string(), 0.5);

        let mut volatilities = HashMap::new();
        volatilities.insert("Market".to_string(), 0.15);
        volatilities.insert("Size".to_string(), 0.10);

        let summary = generate_risk_summary(
            "Portfolio".to_string(),
            start,
            end,
            exposures,
            volatilities,
            0.05,
        );

        assert_eq!(summary.name, "Portfolio");
        assert!(summary.total_risk > 0.0);
        assert!(summary.factor_risk > 0.0);
        assert_eq!(summary.specific_risk, 0.05);
        assert_eq!(summary.factor_contributions.len(), 2);
    }

    #[test]
    fn test_risk_summary_ascii_table() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let factors = vec![FactorRiskContribution::new(
            "Market".to_string(),
            1.2,
            0.15,
            0.018,
            0.20,
        )];

        let summary = RiskSummary::new(
            "Portfolio".to_string(),
            start,
            end,
            0.20,
            0.18,
            0.05,
            factors,
        );

        let table = summary.to_ascii_table();
        assert!(table.contains("Portfolio"));
        assert!(table.contains("Total Risk"));
        assert!(table.contains("Market"));
    }

    #[test]
    fn test_risk_summary_markdown() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let factors = vec![FactorRiskContribution::new(
            "Market".to_string(),
            1.2,
            0.15,
            0.018,
            0.20,
        )];

        let summary = RiskSummary::new(
            "Portfolio".to_string(),
            start,
            end,
            0.20,
            0.18,
            0.05,
            factors,
        );

        let md = summary.to_markdown();
        assert!(md.contains("# Risk Summary"));
        assert!(md.contains("## Factor Risk Contributions"));
        assert!(md.contains("| Market |"));
    }

    #[test]
    fn test_factor_risk_contribution_display() {
        let risk = FactorRiskContribution::new("Market".to_string(), 1.2, 0.15, 0.018, 0.20);

        let display = format!("{}", risk);
        assert!(display.contains("Market"));
        assert!(display.contains("exposure"));
    }

    #[test]
    fn test_risk_summary_display() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

        let summary = RiskSummary::new(
            "Portfolio".to_string(),
            start,
            end,
            0.20,
            0.18,
            0.05,
            vec![],
        );

        let display = format!("{}", summary);
        assert!(display.contains("Portfolio"));
        assert!(display.contains("Total Risk"));
    }
}
