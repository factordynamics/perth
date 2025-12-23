//! Sales Growth Factor
//!
//! Measures year-over-year or quarter-over-quarter revenue/sales growth.
//! Often more stable than earnings growth and indicates top-line momentum.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the SalesGrowth factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesGrowthConfig {
    /// Lookback period in quarters (default: 4 for YoY)
    pub periods: usize,
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for SalesGrowthConfig {
    fn default() -> Self {
        Self {
            periods: 4,
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// SalesGrowth computes year-over-year or quarter-over-quarter revenue growth
#[derive(Debug)]
pub struct SalesGrowthFactor {
    config: SalesGrowthConfig,
}

impl Factor for SalesGrowthFactor {
    fn name(&self) -> &str {
        "sales_growth"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        let periods = self.config.periods as i64;

        // Sort by symbol and date to ensure proper shifting
        let mut result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([
                // Get lagged sales value
                col("sales")
                    .shift(lit(periods))
                    .over([col("symbol")])
                    .alias("sales_lag"),
            ])
            // Compute growth rate: (sales_t - sales_t-n) / sales_t-n
            // Handle zero/negative sales by setting to null
            .with_columns([when(col("sales_lag").gt(0.0))
                .then((col("sales") - col("sales_lag")) / col("sales_lag"))
                .otherwise(lit(NULL))
                .alias("growth_rate")]);

        // Apply winsorization if configured
        if self.config.winsorize {
            let pct = self.config.winsorize_pct;
            result = result
                .with_columns([
                    col("growth_rate")
                        .quantile(lit(pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("lower_bound"),
                    col("growth_rate")
                        .quantile(lit(1.0 - pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("upper_bound"),
                ])
                .with_columns([when(col("growth_rate").lt(col("lower_bound")))
                    .then(col("lower_bound"))
                    .when(col("growth_rate").gt(col("upper_bound")))
                    .then(col("upper_bound"))
                    .otherwise(col("growth_rate"))
                    .alias("growth_rate_winsorized")]);
        } else {
            result = result.with_columns([col("growth_rate").alias("growth_rate_winsorized")]);
        }

        // Cross-sectional standardization by date
        result = result
            .with_columns([
                col("growth_rate_winsorized")
                    .mean()
                    .over([col("date")])
                    .alias("growth_mean"),
                col("growth_rate_winsorized")
                    .std(1)
                    .over([col("date")])
                    .alias("growth_std"),
            ])
            .with_columns([when(col("growth_std").gt(0.0))
                .then((col("growth_rate_winsorized") - col("growth_mean")) / col("growth_std"))
                .otherwise(lit(0.0))
                .alias("sales_growth_score")])
            .select([col("symbol"), col("date"), col("sales_growth_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "sales"]
    }
}

impl StyleFactor for SalesGrowthFactor {
    type Config = SalesGrowthConfig;

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

impl Default for SalesGrowthFactor {
    fn default() -> Self {
        Self::with_config(SalesGrowthConfig::default())
    }
}
