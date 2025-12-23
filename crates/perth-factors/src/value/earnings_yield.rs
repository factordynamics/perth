//! Earnings Yield Factor
//!
//! Measures the ratio of earnings to market price (inverse of P/E ratio).
//! Higher values indicate potentially undervalued securities based on earnings power.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the EarningsYield factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EarningsYieldConfig {
    /// Use trailing twelve months (TTM) earnings (default: true)
    pub use_ttm: bool,
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for EarningsYieldConfig {
    fn default() -> Self {
        Self {
            use_ttm: true,
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// EarningsYield computes the ratio of earnings to market price
#[derive(Debug)]
pub struct EarningsYieldFactor {
    config: EarningsYieldConfig,
}

impl Factor for EarningsYieldFactor {
    fn name(&self) -> &str {
        "earnings_yield"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Step 1: Compute earnings / market_cap (inverse of P/E)
        // Handle edge cases: zero or negative market_cap -> null
        // Keep negative earnings as-is (they represent losses)
        let mut result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([when(col("market_cap").gt(lit(0.0)))
                .then(col("earnings") / col("market_cap"))
                .otherwise(lit(NULL))
                .alias("raw_ey")]);

        // Step 2: Winsorize if configured
        if self.config.winsorize {
            let lower_pct = self.config.winsorize_pct;
            let upper_pct = 1.0 - self.config.winsorize_pct;

            result = result
                .with_columns([
                    col("raw_ey")
                        .quantile(lit(lower_pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("ey_lower"),
                    col("raw_ey")
                        .quantile(lit(upper_pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("ey_upper"),
                ])
                .with_columns([when(col("raw_ey").lt(col("ey_lower")))
                    .then(col("ey_lower"))
                    .when(col("raw_ey").gt(col("ey_upper")))
                    .then(col("ey_upper"))
                    .otherwise(col("raw_ey"))
                    .alias("winsorized_ey")]);
        } else {
            result = result.with_columns([col("raw_ey").alias("winsorized_ey")]);
        }

        // Step 3: Cross-sectional standardization (mean=0, std=1) by date
        result = result
            .with_columns([
                col("winsorized_ey")
                    .mean()
                    .over([col("date")])
                    .alias("ey_mean"),
                col("winsorized_ey")
                    .std(1)
                    .over([col("date")])
                    .alias("ey_std"),
            ])
            .with_columns([((col("winsorized_ey") - col("ey_mean")) / col("ey_std"))
                .alias("earnings_yield_score")])
            .select([col("symbol"), col("date"), col("earnings_yield_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "earnings", "market_cap"]
    }
}

impl StyleFactor for EarningsYieldFactor {
    type Config = EarningsYieldConfig;

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

impl Default for EarningsYieldFactor {
    fn default() -> Self {
        Self::with_config(EarningsYieldConfig::default())
    }
}
