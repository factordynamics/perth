//! Earnings Growth Factor
//!
//! Measures year-over-year or quarter-over-quarter earnings growth.
//! Higher growth indicates stronger business momentum and expansion.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_math::{center_xsection, winsorize_xsection};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the EarningsGrowth factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EarningsGrowthConfig {
    /// Lookback period in quarters (default: 4 for YoY)
    pub periods: usize,
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for EarningsGrowthConfig {
    fn default() -> Self {
        Self {
            periods: 4,
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// EarningsGrowth computes year-over-year or quarter-over-quarter earnings growth
#[derive(Debug)]
pub struct EarningsGrowthFactor {
    config: EarningsGrowthConfig,
}

impl Factor for EarningsGrowthFactor {
    fn name(&self) -> &str {
        "earnings_growth"
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
                // Get lagged earnings value
                col("earnings")
                    .shift(lit(periods))
                    .over([col("symbol")])
                    .alias("earnings_lag"),
            ])
            // Compute growth rate: (earnings_t - earnings_t-n) / abs(earnings_t-n)
            .with_columns([when(col("earnings_lag").fill_null(0).neq(0.0))
                .then(
                    (col("earnings") - col("earnings_lag"))
                        / when(col("earnings_lag").lt(0.0))
                            .then(-col("earnings_lag"))
                            .otherwise(col("earnings_lag")),
                )
                .otherwise(lit(NULL))
                .alias("growth_rate")]);

        // Apply winsorization if configured using toraniko-math
        if self.config.winsorize {
            result =
                winsorize_xsection(result, &["growth_rate"], "date", self.config.winsorize_pct);
        }

        // Cross-sectional standardization by date using toraniko-math
        let result = result
            .with_columns([
                center_xsection("growth_rate", "date", true).alias("earnings_growth_score")
            ])
            .select([col("symbol"), col("date"), col("earnings_growth_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "earnings"]
    }
}

impl StyleFactor for EarningsGrowthFactor {
    type Config = EarningsGrowthConfig;

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

impl Default for EarningsGrowthFactor {
    fn default() -> Self {
        Self::with_config(EarningsGrowthConfig::default())
    }
}
