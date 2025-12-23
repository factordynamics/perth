//! Composite Quality Factor
//!
//! Combines ROE and leverage (inverted) into a single quality score.
//! Captures both profitability and financial stability dimensions of quality.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the CompositeQuality factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeQualityConfig {
    /// Weight for ROE (default: 0.6)
    pub roe_weight: f64,
    /// Weight for leverage (default: 0.4)
    pub leverage_weight: f64,
}

impl Default for CompositeQualityConfig {
    fn default() -> Self {
        Self {
            roe_weight: 0.6,
            leverage_weight: 0.4,
        }
    }
}

/// CompositeQuality computes a combined quality signal from ROE and leverage
#[derive(Debug)]
pub struct CompositeQualityFactor {
    config: CompositeQualityConfig,
}

impl Factor for CompositeQualityFactor {
    fn name(&self) -> &str {
        "composite_quality"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Step 1: Compute ROE = net_income / shareholders_equity
        let result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([(col("net_income") / col("shareholders_equity")).alias("roe_raw")])
            .with_columns([when(col("shareholders_equity").gt(0.0))
                .then(col("roe_raw"))
                .otherwise(lit(NULL))
                .alias("roe_clean")]);

        // Step 2: Compute Leverage = total_debt / shareholders_equity
        let result = result
            .with_columns([(col("total_debt") / col("shareholders_equity")).alias("leverage_raw")])
            .with_columns([when(col("shareholders_equity").gt(0.0))
                .then(col("leverage_raw"))
                .otherwise(lit(NULL))
                .alias("leverage_clean")]);

        // Step 3: Invert leverage (lower leverage = higher quality)
        let result =
            result.with_columns([(lit(-1.0) * col("leverage_clean")).alias("leverage_inverted")]);

        // Step 4: Standardize each component cross-sectionally by date
        let result = result
            .with_columns([
                // Standardize ROE
                col("roe_clean")
                    .mean()
                    .over([col("date")])
                    .alias("roe_mean"),
                col("roe_clean").std(1).over([col("date")]).alias("roe_std"),
                // Standardize inverted leverage
                col("leverage_inverted")
                    .mean()
                    .over([col("date")])
                    .alias("leverage_mean"),
                col("leverage_inverted")
                    .std(1)
                    .over([col("date")])
                    .alias("leverage_std"),
            ])
            .with_columns([
                ((col("roe_clean") - col("roe_mean")) / col("roe_std")).alias("roe_standardized"),
                ((col("leverage_inverted") - col("leverage_mean")) / col("leverage_std"))
                    .alias("leverage_standardized"),
            ]);

        // Step 5: Weighted average based on config
        let roe_weight = self.config.roe_weight;
        let leverage_weight = self.config.leverage_weight;

        let result = result.with_columns([(lit(roe_weight) * col("roe_standardized")
            + lit(leverage_weight) * col("leverage_standardized"))
        .alias("composite_raw")]);

        // Step 6: Final cross-sectional standardization
        let result = result
            .with_columns([
                col("composite_raw")
                    .mean()
                    .over([col("date")])
                    .alias("composite_mean"),
                col("composite_raw")
                    .std(1)
                    .over([col("date")])
                    .alias("composite_std"),
            ])
            .with_columns([
                ((col("composite_raw") - col("composite_mean")) / col("composite_std"))
                    .alias("composite_quality_score"),
            ])
            .select([col("symbol"), col("date"), col("composite_quality_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &[
            "symbol",
            "date",
            "net_income",
            "shareholders_equity",
            "total_debt",
        ]
    }
}

impl StyleFactor for CompositeQualityFactor {
    type Config = CompositeQualityConfig;

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

impl Default for CompositeQualityFactor {
    fn default() -> Self {
        Self::with_config(CompositeQualityConfig::default())
    }
}
