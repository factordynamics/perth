//! Short-Term Momentum Factor
//!
//! Measures price momentum over a short lookback period (typically 1 month).
//! Captures recent price trends and potential mean reversion effects.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the ShortTermMomentum factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortTermMomentumConfig {
    /// Lookback window in days (default: 21 for ~1 month)
    pub lookback: usize,
    /// Skip most recent days to avoid bid-ask bounce (default: 0)
    pub skip_days: usize,
}

impl Default for ShortTermMomentumConfig {
    fn default() -> Self {
        Self {
            lookback: 21,
            skip_days: 0,
        }
    }
}

/// ShortTermMomentum computes price momentum over a 1-month lookback period
#[derive(Debug)]
pub struct ShortTermMomentumFactor {
    config: ShortTermMomentumConfig,
}

impl Factor for ShortTermMomentumFactor {
    fn name(&self) -> &str {
        "short_term_momentum"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let lookback = self.config.lookback;
        let skip_days = self.config.skip_days;

        // Compute 1-month cumulative return
        // 1. Sort data by symbol and date
        // 2. Skip most recent days if configured
        // 3. Compute rolling sum of returns over lookback window
        // 4. Cross-sectionally standardize by date
        let result =
            data.sort(["symbol", "date"], Default::default())
                .with_columns([
                    // Skip most recent days by shifting returns forward
                    col("returns")
                        .shift(lit(skip_days as i64))
                        .over([col("symbol")])
                        .alias("shifted_returns"),
                ])
                .with_columns([
                    // Compute cumulative return over lookback period
                    col("shifted_returns")
                        .rolling_sum(RollingOptionsFixedWindow {
                            window_size: lookback,
                            min_periods: lookback,
                            ..Default::default()
                        })
                        .over([col("symbol")])
                        .alias("cum_return"),
                ])
                // Cross-sectional standardization by date
                .with_columns([
                    col("cum_return")
                        .mean()
                        .over([col("date")])
                        .alias("cum_return_mean"),
                    col("cum_return")
                        .std(1)
                        .over([col("date")])
                        .alias("cum_return_std"),
                ])
                .with_columns([((col("cum_return") - col("cum_return_mean"))
                    / col("cum_return_std"))
                .alias("short_term_momentum_score")])
                .select([col("symbol"), col("date"), col("short_term_momentum_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "price", "returns"]
    }
}

impl StyleFactor for ShortTermMomentumFactor {
    type Config = ShortTermMomentumConfig;

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

impl Default for ShortTermMomentumFactor {
    fn default() -> Self {
        Self::with_config(ShortTermMomentumConfig::default())
    }
}
