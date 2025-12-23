//! Long-Term Momentum Factor
//!
//! Measures price momentum over a long lookback period (typically 12 months).
//! Captures persistent long-term trends in security prices.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the LongTermMomentum factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LongTermMomentumConfig {
    /// Lookback window in days (default: 252 for ~12 months)
    pub lookback: usize,
    /// Skip most recent days to avoid reversal (default: 21)
    pub skip_days: usize,
}

impl Default for LongTermMomentumConfig {
    fn default() -> Self {
        Self {
            lookback: 252,
            skip_days: 21,
        }
    }
}

/// LongTermMomentum computes price momentum over a 12-month lookback period
#[derive(Debug)]
pub struct LongTermMomentumFactor {
    config: LongTermMomentumConfig,
}

impl Factor for LongTermMomentumFactor {
    fn name(&self) -> &str {
        "long_term_momentum"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let lookback = self.config.lookback;
        let skip_days = self.config.skip_days;

        // Compute 12-month cumulative return (12-1 month momentum)
        // 1. Sort data by symbol and date
        // 2. Skip most recent 21 days to avoid short-term reversal
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
                .alias("long_term_momentum_score")])
                .select([col("symbol"), col("date"), col("long_term_momentum_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "price", "returns"]
    }
}

impl StyleFactor for LongTermMomentumFactor {
    type Config = LongTermMomentumConfig;

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

impl Default for LongTermMomentumFactor {
    fn default() -> Self {
        Self::with_config(LongTermMomentumConfig::default())
    }
}
