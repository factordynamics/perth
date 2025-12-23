//! Leverage Factor
//!
//! Measures financial leverage (debt-to-equity ratio). Lower leverage typically
//! indicates higher quality and financial stability. Negative score = high leverage (lower quality).

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the Leverage factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeverageConfig {
    /// Use total debt or long-term debt only (default: total)
    pub use_total_debt: bool,
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for LeverageConfig {
    fn default() -> Self {
        Self {
            use_total_debt: true,
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// Leverage computes financial leverage as debt-to-equity ratio
#[derive(Debug)]
pub struct LeverageFactor {
    config: LeverageConfig,
}

impl Factor for LeverageFactor {
    fn name(&self) -> &str {
        "leverage"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Compute leverage = total_debt / shareholders_equity
        // Handle negative/zero equity by setting to null
        let result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([
                // Compute raw leverage ratio
                (col("total_debt") / col("shareholders_equity")).alias("leverage_raw"),
            ])
            .with_columns([
                // Handle negative or zero equity: set to null if equity <= 0
                when(col("shareholders_equity").gt(0.0))
                    .then(col("leverage_raw"))
                    .otherwise(lit(NULL))
                    .alias("leverage_clean"),
            ]);

        // Apply winsorization if configured
        let result = if self.config.winsorize {
            let pct = self.config.winsorize_pct;
            result
                .with_columns([
                    col("leverage_clean")
                        .quantile(lit(pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("leverage_lower"),
                    col("leverage_clean")
                        .quantile(lit(1.0 - pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("leverage_upper"),
                ])
                .with_columns([when(col("leverage_clean").is_null())
                    .then(lit(NULL))
                    .when(col("leverage_clean").lt(col("leverage_lower")))
                    .then(col("leverage_lower"))
                    .when(col("leverage_clean").gt(col("leverage_upper")))
                    .then(col("leverage_upper"))
                    .otherwise(col("leverage_clean"))
                    .alias("leverage_winsorized")])
        } else {
            result.with_columns([col("leverage_clean").alias("leverage_winsorized")])
        };

        // Invert sign: lower leverage = higher quality score
        // Cross-sectional standardization by date
        let result = result
            .with_columns([(lit(-1.0) * col("leverage_winsorized")).alias("leverage_inverted")])
            .with_columns([
                col("leverage_inverted")
                    .mean()
                    .over([col("date")])
                    .alias("leverage_mean"),
                col("leverage_inverted")
                    .std(1)
                    .over([col("date")])
                    .alias("leverage_std"),
            ])
            .with_columns([((col("leverage_inverted") - col("leverage_mean"))
                / col("leverage_std"))
            .alias("leverage_score")])
            .select([col("symbol"), col("date"), col("leverage_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "total_debt", "shareholders_equity"]
    }
}

impl StyleFactor for LeverageFactor {
    type Config = LeverageConfig;

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

impl Default for LeverageFactor {
    fn default() -> Self {
        Self::with_config(LeverageConfig::default())
    }
}
