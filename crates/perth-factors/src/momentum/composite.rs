//! Composite Momentum Factor
//!
//! Combines short, medium, and long-term momentum signals into a single
//! composite score. Allows for customizable weighting of different horizons.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the CompositeMomentum factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeMomentumConfig {
    /// Weight for short-term momentum (default: 0.2)
    pub short_term_weight: f64,
    /// Weight for medium-term momentum (default: 0.5)
    pub medium_term_weight: f64,
    /// Weight for long-term momentum (default: 0.3)
    pub long_term_weight: f64,
}

impl Default for CompositeMomentumConfig {
    fn default() -> Self {
        Self {
            short_term_weight: 0.2,
            medium_term_weight: 0.5,
            long_term_weight: 0.3,
        }
    }
}

/// CompositeMomentum computes a combined momentum signal from short, medium, and long-term trends
#[derive(Debug)]
pub struct CompositeMomentumFactor {
    config: CompositeMomentumConfig,
}

impl Factor for CompositeMomentumFactor {
    fn name(&self) -> &str {
        "composite_momentum"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let short_weight = self.config.short_term_weight;
        let medium_weight = self.config.medium_term_weight;
        let long_weight = self.config.long_term_weight;

        // Compute composite momentum by combining short, medium, and long-term signals
        // 1. Compute each momentum component
        // 2. Standardize each component cross-sectionally
        // 3. Weighted average
        // 4. Final cross-sectional standardization

        let result = data
            .sort(["symbol", "date"], Default::default())
            // Short-term momentum (1 month, no skip)
            .with_columns([col("returns")
                .rolling_sum(RollingOptionsFixedWindow {
                    window_size: 21,
                    min_periods: 21,
                    ..Default::default()
                })
                .over([col("symbol")])
                .alias("short_momentum_raw")])
            // Medium-term momentum (6 months, skip 21 days)
            .with_columns([col("returns")
                .shift(lit(21))
                .over([col("symbol")])
                .alias("shifted_returns_21")])
            .with_columns([col("shifted_returns_21")
                .rolling_sum(RollingOptionsFixedWindow {
                    window_size: 126,
                    min_periods: 126,
                    ..Default::default()
                })
                .over([col("symbol")])
                .alias("medium_momentum_raw")])
            // Long-term momentum (12 months, skip 21 days)
            .with_columns([col("shifted_returns_21")
                .rolling_sum(RollingOptionsFixedWindow {
                    window_size: 252,
                    min_periods: 252,
                    ..Default::default()
                })
                .over([col("symbol")])
                .alias("long_momentum_raw")])
            // Standardize each component cross-sectionally by date
            .with_columns([
                // Short-term standardization
                col("short_momentum_raw")
                    .mean()
                    .over([col("date")])
                    .alias("short_mean"),
                col("short_momentum_raw")
                    .std(1)
                    .over([col("date")])
                    .alias("short_std"),
                // Medium-term standardization
                col("medium_momentum_raw")
                    .mean()
                    .over([col("date")])
                    .alias("medium_mean"),
                col("medium_momentum_raw")
                    .std(1)
                    .over([col("date")])
                    .alias("medium_std"),
                // Long-term standardization
                col("long_momentum_raw")
                    .mean()
                    .over([col("date")])
                    .alias("long_mean"),
                col("long_momentum_raw")
                    .std(1)
                    .over([col("date")])
                    .alias("long_std"),
            ])
            .with_columns([
                ((col("short_momentum_raw") - col("short_mean")) / col("short_std"))
                    .alias("short_momentum_std"),
                ((col("medium_momentum_raw") - col("medium_mean")) / col("medium_std"))
                    .alias("medium_momentum_std"),
                ((col("long_momentum_raw") - col("long_mean")) / col("long_std"))
                    .alias("long_momentum_std"),
            ])
            // Weighted average of standardized components
            .with_columns([(lit(short_weight) * col("short_momentum_std")
                + lit(medium_weight) * col("medium_momentum_std")
                + lit(long_weight) * col("long_momentum_std"))
            .alias("composite_momentum_raw")])
            // Final cross-sectional standardization
            .with_columns([
                col("composite_momentum_raw")
                    .mean()
                    .over([col("date")])
                    .alias("composite_mean"),
                col("composite_momentum_raw")
                    .std(1)
                    .over([col("date")])
                    .alias("composite_std"),
            ])
            .with_columns([((col("composite_momentum_raw") - col("composite_mean"))
                / col("composite_std"))
            .alias("composite_momentum_score")])
            .select([col("symbol"), col("date"), col("composite_momentum_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "price", "returns"]
    }
}

impl StyleFactor for CompositeMomentumFactor {
    type Config = CompositeMomentumConfig;

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

impl Default for CompositeMomentumFactor {
    fn default() -> Self {
        Self::with_config(CompositeMomentumConfig::default())
    }
}
