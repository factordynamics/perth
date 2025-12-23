//! Medium-Term Momentum Factor
//!
//! Measures price momentum over a medium lookback period (typically 6 months).
//! This is the classic momentum effect studied in academic literature.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the MediumTermMomentum factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediumTermMomentumConfig {
    /// Lookback window in days (default: 126 for ~6 months)
    pub lookback: usize,
    /// Skip most recent days to avoid reversal (default: 21)
    pub skip_days: usize,
}

impl Default for MediumTermMomentumConfig {
    fn default() -> Self {
        Self {
            lookback: 126,
            skip_days: 21,
        }
    }
}

/// MediumTermMomentum computes price momentum over a 6-month lookback period
#[derive(Debug)]
pub struct MediumTermMomentumFactor {
    config: MediumTermMomentumConfig,
}

impl Factor for MediumTermMomentumFactor {
    fn name(&self) -> &str {
        "medium_term_momentum"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let lookback = self.config.lookback;
        let skip_days = self.config.skip_days;

        // Compute 6-month cumulative return
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
                .alias("medium_term_momentum_score")])
                .select([
                    col("symbol"),
                    col("date"),
                    col("medium_term_momentum_score"),
                ]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "price", "returns"]
    }
}

impl StyleFactor for MediumTermMomentumFactor {
    type Config = MediumTermMomentumConfig;

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

impl Default for MediumTermMomentumFactor {
    fn default() -> Self {
        Self::with_config(MediumTermMomentumConfig::default())
    }
}
