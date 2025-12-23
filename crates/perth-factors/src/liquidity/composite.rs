//! Composite Liquidity Factor
//!
//! Combines multiple liquidity measures into a single composite score.
//! Inverts Amihud illiquidity so higher = more liquid (consistent with turnover).
//! Higher composite values indicate higher overall liquidity.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_math::center_xsection;
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the CompositeLiquidity factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeLiquidityConfig {
    /// Rolling window for liquidity metrics (default: 21 days)
    pub window: usize,
    /// Minimum periods for calculation (default: 10)
    pub min_periods: usize,
    /// Scale factor for Amihud (default: 1e6)
    pub amihud_scale: f64,
    /// Weight for turnover component (default: 0.5)
    pub turnover_weight: f64,
    /// Weight for inverted Amihud component (default: 0.5)
    pub amihud_weight: f64,
}

impl Default for CompositeLiquidityConfig {
    fn default() -> Self {
        Self {
            window: 21,
            min_periods: 10,
            amihud_scale: 1_000_000.0,
            turnover_weight: 0.5,
            amihud_weight: 0.5,
        }
    }
}

/// CompositeLiquidity computes a combined liquidity signal from turnover and Amihud illiquidity
#[derive(Debug)]
pub struct CompositeLiquidityFactor {
    config: CompositeLiquidityConfig,
}

impl Factor for CompositeLiquidityFactor {
    fn name(&self) -> &str {
        "composite_liquidity"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let window = self.config.window;
        let min_periods = self.config.min_periods;
        let scale = self.config.amihud_scale;
        let turnover_weight = self.config.turnover_weight;
        let amihud_weight = self.config.amihud_weight;

        let result =
            data.sort(["symbol", "date"], Default::default())
                .with_columns([
                    // Compute dollar volume for Amihud
                    (col("price") * col("volume")).alias("dollar_volume"),
                    // Compute daily turnover
                    (col("volume") / col("shares_outstanding")).alias("daily_turnover"),
                ])
                .with_columns([
                    // Compute daily illiquidity = abs(return) / dollar_volume
                    // Use conditional to compute absolute value
                    when(col("returns").lt(0.0))
                        .then(-col("returns") / col("dollar_volume") * lit(scale))
                        .otherwise(col("returns") / col("dollar_volume") * lit(scale))
                        .alias("daily_illiquidity"),
                ])
                .with_columns([
                    // Calculate rolling average for Amihud
                    col("daily_illiquidity")
                        .rolling_mean(RollingOptionsFixedWindow {
                            window_size: window,
                            min_periods,
                            ..Default::default()
                        })
                        .over([col("symbol")])
                        .alias("raw_amihud"),
                    // Calculate rolling average for turnover
                    col("daily_turnover")
                        .rolling_mean(RollingOptionsFixedWindow {
                            window_size: window,
                            min_periods,
                            ..Default::default()
                        })
                        .over([col("symbol")])
                        .alias("raw_turnover"),
                ])
                // Cross-sectional standardization using toraniko-math
                .with_columns([
                    center_xsection("raw_amihud", "date", true).alias("amihud_score"),
                    center_xsection("raw_turnover", "date", true).alias("turnover_score"),
                ])
                // Invert Amihud so higher = more liquid
                // Combine with weights: composite = turnover_weight * turnover - amihud_weight * amihud
                .with_columns([(col("turnover_score") * lit(turnover_weight)
                    - col("amihud_score") * lit(amihud_weight))
                .alias("raw_composite")])
                // Final cross-sectional standardization of composite using toraniko-math
                .with_columns([center_xsection("raw_composite", "date", true)
                    .alias("composite_liquidity_score")])
                .select([col("symbol"), col("date"), col("composite_liquidity_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &[
            "symbol",
            "date",
            "returns",
            "price",
            "volume",
            "shares_outstanding",
        ]
    }
}

impl StyleFactor for CompositeLiquidityFactor {
    type Config = CompositeLiquidityConfig;

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

impl Default for CompositeLiquidityFactor {
    fn default() -> Self {
        Self::with_config(CompositeLiquidityConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_name() {
        let factor = CompositeLiquidityFactor::default();
        assert_eq!(factor.name(), "composite_liquidity");
        assert_eq!(factor.kind(), FactorKind::Style);
    }

    #[test]
    fn test_required_columns() {
        let factor = CompositeLiquidityFactor::default();
        let cols = factor.required_columns();
        assert_eq!(cols.len(), 6);
        assert!(cols.contains(&"symbol"));
        assert!(cols.contains(&"date"));
        assert!(cols.contains(&"returns"));
        assert!(cols.contains(&"price"));
        assert!(cols.contains(&"volume"));
        assert!(cols.contains(&"shares_outstanding"));
    }

    #[test]
    fn test_config_defaults() {
        let config = CompositeLiquidityConfig::default();
        assert_eq!(config.window, 21);
        assert_eq!(config.min_periods, 10);
        assert_eq!(config.amihud_scale, 1_000_000.0);
        assert_eq!(config.turnover_weight, 0.5);
        assert_eq!(config.amihud_weight, 0.5);
    }

    #[test]
    fn test_custom_config() {
        let config = CompositeLiquidityConfig {
            window: 42,
            min_periods: 20,
            amihud_scale: 1_000.0,
            turnover_weight: 0.6,
            amihud_weight: 0.4,
        };
        let factor = CompositeLiquidityFactor::with_config(config);
        assert_eq!(factor.config().window, 42);
        assert_eq!(factor.config().min_periods, 20);
        assert_eq!(factor.config().amihud_scale, 1_000.0);
        assert_eq!(factor.config().turnover_weight, 0.6);
        assert_eq!(factor.config().amihud_weight, 0.4);
    }

    #[test]
    fn test_residualize() {
        let factor = CompositeLiquidityFactor::default();
        assert!(factor.residualize());
    }
}
