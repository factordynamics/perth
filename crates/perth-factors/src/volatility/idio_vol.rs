//! Idiosyncratic Volatility Factor
//!
//! Measures stock-specific volatility after removing market risk.
//! Computed as the standard deviation of residuals from a market model regression.
//!
//! Lower idiosyncratic volatility often indicates higher quality, more stable stocks.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the IdioVol factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdioVolConfig {
    /// Rolling window size for beta estimation (default: 63 days)
    pub window: usize,
    /// Minimum number of observations required (default: 20)
    pub min_periods: usize,
    /// Name of the market return column (default: "market_return")
    pub market_column: String,
}

impl Default for IdioVolConfig {
    fn default() -> Self {
        Self {
            window: 63,
            min_periods: 20,
            market_column: "market_return".to_string(),
        }
    }
}

/// IdiosyncraticVolatility computes stock-specific volatility after removing market risk
#[derive(Debug)]
pub struct IdiosyncraticVolatilityFactor {
    config: IdioVolConfig,
}

impl Factor for IdiosyncraticVolatilityFactor {
    fn name(&self) -> &str {
        "idiosyncratic_volatility"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let window = self.config.window;
        let min_periods = self.config.min_periods;
        let market_col = &self.config.market_column;

        // Step 1: Estimate beta using rolling regression
        // Beta = Cov(R_i, R_m) / Var(R_m)
        let result =
            data.sort(["symbol", "date"], Default::default())
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
                // Compute covariance: E[(r - mean_r)(m - mean_m)]
                .with_columns([((col("returns") - col("returns_mean"))
                    * (col(market_col) - col("market_mean")))
                .rolling_mean(RollingOptionsFixedWindow {
                    window_size: window,
                    min_periods,
                    ..Default::default()
                })
                .over([col("symbol")])
                .alias("covariance")])
                // Beta = covariance / variance_market
                .with_columns([
                    (col("covariance") / (col("market_std") * col("market_std"))).alias("beta"),
                ])
                // Step 2: Compute residuals from market model
                // residual = return - beta * market_return
                .with_columns([(col("returns") - col("beta") * col(market_col)).alias("residual")])
                // Step 3: Calculate rolling standard deviation of residuals
                .with_columns([col("residual")
                    .rolling_std(RollingOptionsFixedWindow {
                        window_size: window,
                        min_periods,
                        ..Default::default()
                    })
                    .over([col("symbol")])
                    .alias("raw_idio_vol")])
                // Step 4: Cross-sectional standardization by date
                .with_columns([
                    col("raw_idio_vol")
                        .mean()
                        .over([col("date")])
                        .alias("idio_vol_mean"),
                    col("raw_idio_vol")
                        .std(1)
                        .over([col("date")])
                        .alias("idio_vol_std"),
                ])
                .with_columns([((col("raw_idio_vol") - col("idio_vol_mean"))
                    / col("idio_vol_std"))
                .alias("idiosyncratic_volatility_score")])
                .select([
                    col("symbol"),
                    col("date"),
                    col("idiosyncratic_volatility_score"),
                ]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "returns", "market_return"]
    }
}

impl StyleFactor for IdiosyncraticVolatilityFactor {
    type Config = IdioVolConfig;

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

impl Default for IdiosyncraticVolatilityFactor {
    fn default() -> Self {
        Self::with_config(IdioVolConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_name() {
        let factor = IdiosyncraticVolatilityFactor::default();
        assert_eq!(factor.name(), "idiosyncratic_volatility");
        assert_eq!(factor.kind(), FactorKind::Style);
    }

    #[test]
    fn test_required_columns() {
        let factor = IdiosyncraticVolatilityFactor::default();
        let cols = factor.required_columns();
        assert_eq!(cols.len(), 4);
        assert!(cols.contains(&"symbol"));
        assert!(cols.contains(&"date"));
        assert!(cols.contains(&"returns"));
        assert!(cols.contains(&"market_return"));
    }

    #[test]
    fn test_config_defaults() {
        let config = IdioVolConfig::default();
        assert_eq!(config.window, 63);
        assert_eq!(config.min_periods, 20);
        assert_eq!(config.market_column, "market_return");
    }

    #[test]
    fn test_custom_config() {
        let config = IdioVolConfig {
            window: 126,
            min_periods: 30,
            market_column: "spy_return".to_string(),
        };
        let factor = IdiosyncraticVolatilityFactor::with_config(config);
        assert_eq!(factor.config().window, 126);
        assert_eq!(factor.config().min_periods, 30);
        assert_eq!(factor.config().market_column, "spy_return");
    }

    #[test]
    fn test_residualize() {
        let factor = IdiosyncraticVolatilityFactor::default();
        assert!(factor.residualize());
    }
}
