//! Amihud Illiquidity Factor
//!
//! Measures price impact per unit of volume: |return| / dollar_volume.
//! Higher values indicate lower liquidity (higher price impact per trade).
//! This captures the liquidity premium directly.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the Amihud factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmihudConfig {
    /// Rolling window for average illiquidity (default: 21 days)
    pub window: usize,
    /// Minimum periods for calculation (default: 10)
    pub min_periods: usize,
    /// Scale factor for readability (default: 1e6)
    pub scale: f64,
}

impl Default for AmihudConfig {
    fn default() -> Self {
        Self {
            window: 21,
            min_periods: 10,
            scale: 1_000_000.0,
        }
    }
}

/// Amihud computes illiquidity as price impact per unit of volume
#[derive(Debug)]
pub struct AmihudFactor {
    config: AmihudConfig,
}

impl Factor for AmihudFactor {
    fn name(&self) -> &str {
        "amihud"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let window = self.config.window;
        let min_periods = self.config.min_periods;
        let scale = self.config.scale;

        let result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([
                // Compute dollar volume = price * volume
                (col("price") * col("volume")).alias("dollar_volume"),
            ])
            .with_columns([
                // Compute daily illiquidity = abs(return) / dollar_volume
                // Scale by scale factor for readability (e.g., 1e6)
                // Use conditional to compute absolute value
                when(col("returns").lt(0.0))
                    .then(-col("returns") / col("dollar_volume") * lit(scale))
                    .otherwise(col("returns") / col("dollar_volume") * lit(scale))
                    .alias("daily_illiquidity"),
            ])
            .with_columns([
                // Calculate rolling average over window
                col("daily_illiquidity")
                    .rolling_mean(RollingOptionsFixedWindow {
                        window_size: window,
                        min_periods,
                        ..Default::default()
                    })
                    .over([col("symbol")])
                    .alias("raw_amihud"),
            ])
            // Cross-sectional standardization by date
            .with_columns([
                col("raw_amihud")
                    .mean()
                    .over([col("date")])
                    .alias("amihud_mean"),
                col("raw_amihud")
                    .std(1)
                    .over([col("date")])
                    .alias("amihud_std"),
            ])
            .with_columns([
                ((col("raw_amihud") - col("amihud_mean")) / col("amihud_std"))
                    .alias("amihud_score"),
            ])
            .select([col("symbol"), col("date"), col("amihud_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "returns", "price", "volume"]
    }
}

impl StyleFactor for AmihudFactor {
    type Config = AmihudConfig;

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

impl Default for AmihudFactor {
    fn default() -> Self {
        Self::with_config(AmihudConfig::default())
    }
}
