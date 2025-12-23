//! Composite Volatility Factor
//!
//! Combines beta, historical volatility, and idiosyncratic volatility into a single composite risk measure.
//! Captures systematic (beta), total (historical volatility), and stock-specific (idiosyncratic) risk.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

use super::beta::BetaFactor;
use super::historical_vol::HistoricalVolatilityFactor;
use super::idio_vol::IdiosyncraticVolatilityFactor;

/// Configuration for the CompositeVolatility factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeVolatilityConfig {
    /// Weight for beta (default: 0.4)
    pub beta_weight: f64,
    /// Weight for historical volatility (default: 0.3)
    pub hist_vol_weight: f64,
    /// Weight for idiosyncratic volatility (default: 0.3)
    pub idio_vol_weight: f64,
}

impl Default for CompositeVolatilityConfig {
    fn default() -> Self {
        Self {
            beta_weight: 0.4,
            hist_vol_weight: 0.3,
            idio_vol_weight: 0.3,
        }
    }
}

/// CompositeVolatility computes a combined risk signal from beta, historical volatility, and idiosyncratic volatility
#[derive(Debug)]
pub struct CompositeVolatilityFactor {
    config: CompositeVolatilityConfig,
}

impl Factor for CompositeVolatilityFactor {
    fn name(&self) -> &str {
        "composite_volatility"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Step 1: Compute beta scores
        let beta_factor = BetaFactor::default();
        let beta_scores = beta_factor.compute_scores(data.clone())?;

        // Step 2: Compute historical volatility scores
        let hist_vol_factor = HistoricalVolatilityFactor::default();
        let hist_vol_scores = hist_vol_factor.compute_scores(data.clone())?;

        // Step 3: Compute idiosyncratic volatility scores
        let idio_vol_factor = IdiosyncraticVolatilityFactor::default();
        let idio_vol_scores = idio_vol_factor.compute_scores(data)?;

        // Step 4: Join all three components
        let combined = beta_scores
            .join(
                hist_vol_scores,
                [col("symbol"), col("date")],
                [col("symbol"), col("date")],
                JoinArgs::new(JoinType::Inner),
            )
            .join(
                idio_vol_scores,
                [col("symbol"), col("date")],
                [col("symbol"), col("date")],
                JoinArgs::new(JoinType::Inner),
            );

        // Step 5: Create weighted composite score
        let beta_weight = self.config.beta_weight;
        let hist_vol_weight = self.config.hist_vol_weight;
        let idio_vol_weight = self.config.idio_vol_weight;

        let result = combined
            .with_columns([(col("beta_score") * lit(beta_weight)
                + col("historical_volatility_score") * lit(hist_vol_weight)
                + col("idiosyncratic_volatility_score") * lit(idio_vol_weight))
            .alias("raw_composite")])
            // Step 6: Final cross-sectional standardization
            .with_columns([
                col("raw_composite")
                    .mean()
                    .over([col("date")])
                    .alias("composite_mean"),
                col("raw_composite")
                    .std(1)
                    .over([col("date")])
                    .alias("composite_std"),
            ])
            .with_columns([
                ((col("raw_composite") - col("composite_mean")) / col("composite_std"))
                    .alias("composite_volatility_score"),
            ])
            .select([
                col("symbol"),
                col("date"),
                col("composite_volatility_score"),
            ]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "returns", "market_return"]
    }
}

impl StyleFactor for CompositeVolatilityFactor {
    type Config = CompositeVolatilityConfig;

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

impl Default for CompositeVolatilityFactor {
    fn default() -> Self {
        Self::with_config(CompositeVolatilityConfig::default())
    }
}
