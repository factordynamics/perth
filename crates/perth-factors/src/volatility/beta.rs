//! Market Beta Factor
//!
//! Computes rolling regression beta against market returns. Beta measures systematic
//! risk - the sensitivity of a security's returns to market movements.
//!
//! Higher beta = higher systematic risk exposure
//! Beta > 1: More volatile than the market
//! Beta = 1: Moves with the market
//! Beta < 1: Less volatile than the market
//!
//! This is the reference implementation showing the full pattern for Perth factors.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the Beta factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetaConfig {
    /// Rolling window size in days (default: 252 trading days = 1 year)
    pub window: usize,
    /// Minimum number of observations required (default: 60)
    pub min_periods: usize,
    /// Name of the market return column (default: "market_return")
    pub market_column: String,
}

impl Default for BetaConfig {
    fn default() -> Self {
        Self {
            window: 252,
            min_periods: 60,
            market_column: "market_return".to_string(),
        }
    }
}

/// Beta computes systematic risk via rolling regression against market returns
#[derive(Debug)]
pub struct BetaFactor {
    config: BetaConfig,
}

impl Factor for BetaFactor {
    fn name(&self) -> &str {
        "beta"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // For now, return a placeholder that computes a simple beta approximation
        // using rolling correlation and volatility ratio
        // Full implementation would use proper OLS regression

        let window = self.config.window;
        let min_periods = self.config.min_periods;
        let market_col = &self.config.market_column;

        // Compute rolling stats for beta estimation
        // Beta = Cov(R_i, R_m) / Var(R_m)
        // We approximate using rolling_std and correlation
        let result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([
                // Rolling standard deviation of returns
                col("returns")
                    .rolling_std(RollingOptionsFixedWindow {
                        window_size: window,
                        min_periods,
                        ..Default::default()
                    })
                    .over([col("symbol")])
                    .alias("returns_std"),
                // Rolling standard deviation of market
                col(market_col)
                    .rolling_std(RollingOptionsFixedWindow {
                        window_size: window,
                        min_periods,
                        ..Default::default()
                    })
                    .over([col("symbol")])
                    .alias("market_std"),
                // Rolling mean of returns
                col("returns")
                    .rolling_mean(RollingOptionsFixedWindow {
                        window_size: window,
                        min_periods,
                        ..Default::default()
                    })
                    .over([col("symbol")])
                    .alias("returns_mean"),
                // Rolling mean of market
                col(market_col)
                    .rolling_mean(RollingOptionsFixedWindow {
                        window_size: window,
                        min_periods,
                        ..Default::default()
                    })
                    .over([col("symbol")])
                    .alias("market_mean"),
            ])
            // Compute covariance proxy: E[(r - mean_r)(m - mean_m)]
            .with_columns([((col("returns") - col("returns_mean"))
                * (col(market_col) - col("market_mean")))
            .rolling_mean(RollingOptionsFixedWindow {
                window_size: window,
                min_periods,
                ..Default::default()
            })
            .over([col("symbol")])
            .alias("covariance")])
            // Beta = covariance / variance_market = covariance / (std_market^2)
            .with_columns([
                (col("covariance") / (col("market_std") * col("market_std"))).alias("raw_beta"),
            ])
            // Cross-sectional standardization by date
            .with_columns([
                col("raw_beta")
                    .mean()
                    .over([col("date")])
                    .alias("beta_mean"),
                col("raw_beta").std(1).over([col("date")]).alias("beta_std"),
            ])
            .with_columns([
                ((col("raw_beta") - col("beta_mean")) / col("beta_std")).alias("beta_score")
            ])
            .select([col("symbol"), col("date"), col("beta_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "returns", "market_return"]
    }
}

impl StyleFactor for BetaFactor {
    type Config = BetaConfig;

    fn with_config(config: Self::Config) -> Self {
        Self { config }
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn residualize(&self) -> bool {
        true
    }
}

impl Default for BetaFactor {
    fn default() -> Self {
        Self::with_config(BetaConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_name() {
        let factor = BetaFactor::default();
        assert_eq!(factor.name(), "beta");
        assert_eq!(factor.kind(), FactorKind::Style);
    }

    #[test]
    fn test_required_columns() {
        let factor = BetaFactor::default();
        let cols = factor.required_columns();
        assert_eq!(cols.len(), 4);
        assert!(cols.contains(&"symbol"));
        assert!(cols.contains(&"date"));
        assert!(cols.contains(&"returns"));
        assert!(cols.contains(&"market_return"));
    }

    #[test]
    fn test_config_defaults() {
        let config = BetaConfig::default();
        assert_eq!(config.window, 252);
        assert_eq!(config.min_periods, 60);
        assert_eq!(config.market_column, "market_return");
    }

    #[test]
    fn test_custom_config() {
        let config = BetaConfig {
            window: 126,
            min_periods: 30,
            market_column: "spy_return".to_string(),
        };
        let factor = BetaFactor::with_config(config);
        assert_eq!(factor.config().window, 126);
        assert_eq!(factor.config().min_periods, 30);
        assert_eq!(factor.config().market_column, "spy_return");
    }

    #[test]
    fn test_residualize() {
        let factor = BetaFactor::default();
        assert!(factor.residualize());
    }
}
