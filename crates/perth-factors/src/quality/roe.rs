//! Return on Equity (ROE) Factor
//!
//! Measures profitability relative to shareholder equity. Higher ROE indicates
//! more efficient use of equity capital and better fundamental quality.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_math::{center_xsection, winsorize_xsection};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the Roe factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoeConfig {
    /// Use trailing twelve months (default: true)
    pub use_ttm: bool,
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for RoeConfig {
    fn default() -> Self {
        Self {
            use_ttm: true,
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// Roe computes return on equity as net income divided by shareholders equity
#[derive(Debug)]
pub struct RoeFactor {
    config: RoeConfig,
}

impl Factor for RoeFactor {
    fn name(&self) -> &str {
        "roe"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Compute ROE = net_income / shareholders_equity
        // Handle negative equity by setting to null
        let mut result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([
                // Compute raw ROE ratio
                (col("net_income") / col("shareholders_equity")).alias("roe_raw"),
            ])
            .with_columns([
                // Handle negative equity: set to null if equity <= 0
                when(col("shareholders_equity").gt(0.0))
                    .then(col("roe_raw"))
                    .otherwise(lit(NULL))
                    .alias("roe_clean"),
            ]);

        // Apply winsorization if configured using toraniko-math
        if self.config.winsorize {
            result = winsorize_xsection(result, &["roe_clean"], "date", self.config.winsorize_pct);
        }

        // Cross-sectional standardization by date using toraniko-math
        let result = result
            .with_columns([center_xsection("roe_clean", "date", true).alias("roe_score")])
            .select([col("symbol"), col("date"), col("roe_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "net_income", "shareholders_equity"]
    }
}

impl StyleFactor for RoeFactor {
    type Config = RoeConfig;

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

impl Default for RoeFactor {
    fn default() -> Self {
        Self::with_config(RoeConfig::default())
    }
}
