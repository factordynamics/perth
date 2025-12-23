//! Composite Growth Factor
//!
//! Combines earnings growth and sales growth into a single composite score.
//! This provides a more robust growth signal by incorporating both bottom-line
//! (earnings) and top-line (sales) growth metrics.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_math::{center_xsection, winsorize_xsection};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the CompositeGrowth factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeGrowthConfig {
    /// Weight for earnings growth component (default: 0.5)
    pub earnings_weight: f64,
    /// Weight for sales growth component (default: 0.5)
    pub sales_weight: f64,
    /// Lookback period in quarters (default: 4 for YoY)
    pub periods: usize,
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for CompositeGrowthConfig {
    fn default() -> Self {
        Self {
            earnings_weight: 0.5,
            sales_weight: 0.5,
            periods: 4,
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// CompositeGrowth computes a combined growth signal from earnings and sales growth
#[derive(Debug)]
pub struct CompositeGrowthFactor {
    config: CompositeGrowthConfig,
}

impl Factor for CompositeGrowthFactor {
    fn name(&self) -> &str {
        "composite_growth"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let periods = self.config.periods as i64;
        let earnings_weight = self.config.earnings_weight;
        let sales_weight = self.config.sales_weight;

        // Sort by symbol and date to ensure proper shifting
        let mut result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([
                // Get lagged values
                col("earnings")
                    .shift(lit(periods))
                    .over([col("symbol")])
                    .alias("earnings_lag"),
                col("sales")
                    .shift(lit(periods))
                    .over([col("symbol")])
                    .alias("sales_lag"),
            ])
            // Compute growth rates
            .with_columns([
                // Earnings growth: (earnings_t - earnings_t-n) / abs(earnings_t-n)
                when(col("earnings_lag").fill_null(0).neq(0.0))
                    .then(
                        (col("earnings") - col("earnings_lag"))
                            / when(col("earnings_lag").lt(0.0))
                                .then(-col("earnings_lag"))
                                .otherwise(col("earnings_lag")),
                    )
                    .otherwise(lit(NULL))
                    .alias("earnings_growth"),
                // Sales growth: (sales_t - sales_t-n) / sales_t-n
                when(col("sales_lag").gt(0.0))
                    .then((col("sales") - col("sales_lag")) / col("sales_lag"))
                    .otherwise(lit(NULL))
                    .alias("sales_growth"),
            ]);

        // Apply winsorization if configured using toraniko-math
        if self.config.winsorize {
            result = winsorize_xsection(
                result,
                &["earnings_growth", "sales_growth"],
                "date",
                self.config.winsorize_pct,
            );
        }

        // Standardize each component separately before combining using toraniko-math
        let result = result
            .with_columns([
                center_xsection("earnings_growth", "date", true).alias("earnings_growth_std"),
                center_xsection("sales_growth", "date", true).alias("sales_growth_std"),
            ])
            // Combine with weights
            .with_columns([(col("earnings_growth_std") * lit(earnings_weight)
                + col("sales_growth_std") * lit(sales_weight))
            .alias("composite_growth_raw")])
            // Final cross-sectional standardization of composite score using toraniko-math
            .with_columns([center_xsection("composite_growth_raw", "date", true)
                .alias("composite_growth_score")])
            .select([col("symbol"), col("date"), col("composite_growth_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "earnings", "sales"]
    }
}

impl StyleFactor for CompositeGrowthFactor {
    type Config = CompositeGrowthConfig;

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

impl Default for CompositeGrowthFactor {
    fn default() -> Self {
        Self::with_config(CompositeGrowthConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_name() {
        let factor = CompositeGrowthFactor::default();
        assert_eq!(factor.name(), "composite_growth");
        assert_eq!(factor.kind(), FactorKind::Style);
    }

    #[test]
    fn test_required_columns() {
        let factor = CompositeGrowthFactor::default();
        let cols = factor.required_columns();
        assert_eq!(cols.len(), 4);
        assert!(cols.contains(&"symbol"));
        assert!(cols.contains(&"date"));
        assert!(cols.contains(&"earnings"));
        assert!(cols.contains(&"sales"));
    }

    #[test]
    fn test_config_defaults() {
        let config = CompositeGrowthConfig::default();
        assert_eq!(config.earnings_weight, 0.5);
        assert_eq!(config.sales_weight, 0.5);
        assert_eq!(config.periods, 4);
        assert!(config.winsorize);
        assert_eq!(config.winsorize_pct, 0.01);
    }

    #[test]
    fn test_custom_config() {
        let config = CompositeGrowthConfig {
            earnings_weight: 0.6,
            sales_weight: 0.4,
            periods: 8,
            winsorize: false,
            winsorize_pct: 0.05,
        };
        let factor = CompositeGrowthFactor::with_config(config);
        assert_eq!(factor.config().earnings_weight, 0.6);
        assert_eq!(factor.config().sales_weight, 0.4);
        assert_eq!(factor.config().periods, 8);
        assert!(!factor.config().winsorize);
    }

    #[test]
    fn test_residualize() {
        let factor = CompositeGrowthFactor::default();
        assert!(factor.residualize());
    }
}
