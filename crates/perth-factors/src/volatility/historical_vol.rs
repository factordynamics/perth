//! Historical Volatility Factor
//!
//! Measures realized volatility of returns over a rolling window.
//! Lower volatility securities tend to exhibit better risk-adjusted returns.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the HistoricalVolatility factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalVolatilityConfig {
    /// Rolling window size in days (default: 63 for ~3 months)
    pub window: usize,
    /// Minimum number of observations (default: 20)
    pub min_periods: usize,
    /// Annualization factor (default: sqrt(252))
    pub annualize: bool,
}

impl Default for HistoricalVolatilityConfig {
    fn default() -> Self {
        Self {
            window: 63,
            min_periods: 20,
            annualize: true,
        }
    }
}

/// HistoricalVolatility computes realized volatility of returns over a rolling window
#[derive(Debug)]
pub struct HistoricalVolatilityFactor {
    config: HistoricalVolatilityConfig,
}

impl Factor for HistoricalVolatilityFactor {
    fn name(&self) -> &str {
        "historical_volatility"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let window = self.config.window;
        let min_periods = self.config.min_periods;
        let annualize = self.config.annualize;

        // Compute rolling standard deviation of returns
        let mut result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([col("returns")
                .rolling_std(RollingOptionsFixedWindow {
                    window_size: window,
                    min_periods,
                    ..Default::default()
                })
                .over([col("symbol")])
                .alias("raw_volatility")]);

        // Annualize if configured: multiply by sqrt(252)
        if annualize {
            let annualization_factor = (252.0_f64).sqrt();
            result = result.with_columns([
                (col("raw_volatility") * lit(annualization_factor)).alias("raw_volatility")
            ]);
        }

        // Cross-sectional standardization by date (mean=0, std=1)
        let result = result
            .with_columns([
                col("raw_volatility")
                    .mean()
                    .over([col("date")])
                    .alias("vol_mean"),
                col("raw_volatility")
                    .std(1)
                    .over([col("date")])
                    .alias("vol_std"),
            ])
            .with_columns(
                [((col("raw_volatility") - col("vol_mean")) / col("vol_std"))
                    .alias("historical_volatility_score")],
            )
            .select([
                col("symbol"),
                col("date"),
                col("historical_volatility_score"),
            ]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "returns"]
    }
}

impl StyleFactor for HistoricalVolatilityFactor {
    type Config = HistoricalVolatilityConfig;

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

impl Default for HistoricalVolatilityFactor {
    fn default() -> Self {
        Self::with_config(HistoricalVolatilityConfig::default())
    }
}
