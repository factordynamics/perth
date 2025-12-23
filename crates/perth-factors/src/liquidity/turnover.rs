//! Turnover Factor
//!
//! Measures trading activity as volume divided by shares outstanding.
//! Higher turnover indicates higher liquidity and easier trading.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the Turnover factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnoverConfig {
    /// Rolling window for average turnover (default: 21 days)
    pub window: usize,
    /// Minimum periods for calculation (default: 10)
    pub min_periods: usize,
}

impl Default for TurnoverConfig {
    fn default() -> Self {
        Self {
            window: 21,
            min_periods: 10,
        }
    }
}

/// Turnover computes trading activity as volume divided by shares outstanding
#[derive(Debug)]
pub struct TurnoverFactor {
    config: TurnoverConfig,
}

impl Factor for TurnoverFactor {
    fn name(&self) -> &str {
        "turnover"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let window = self.config.window;
        let min_periods = self.config.min_periods;

        let result =
            data.sort(["symbol", "date"], Default::default())
                .with_columns([
                    // Compute daily turnover = volume / shares_outstanding
                    (col("volume") / col("shares_outstanding")).alias("daily_turnover"),
                ])
                .with_columns([
                    // Calculate rolling average over window
                    col("daily_turnover")
                        .rolling_mean(RollingOptionsFixedWindow {
                            window_size: window,
                            min_periods,
                            ..Default::default()
                        })
                        .over([col("symbol")])
                        .alias("raw_turnover"),
                ])
                // Cross-sectional standardization by date
                .with_columns([
                    col("raw_turnover")
                        .mean()
                        .over([col("date")])
                        .alias("turnover_mean"),
                    col("raw_turnover")
                        .std(1)
                        .over([col("date")])
                        .alias("turnover_std"),
                ])
                .with_columns([((col("raw_turnover") - col("turnover_mean"))
                    / col("turnover_std"))
                .alias("turnover_score")])
                .select([col("symbol"), col("date"), col("turnover_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "volume", "shares_outstanding"]
    }
}

impl StyleFactor for TurnoverFactor {
    type Config = TurnoverConfig;

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

impl Default for TurnoverFactor {
    fn default() -> Self {
        Self::with_config(TurnoverConfig::default())
    }
}
