//! Composite Value Factor
//!
//! Combines multiple value metrics (book-to-price, earnings yield, etc.)
//! into a single composite score using equal weighting or optimization.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_math::center_xsection;
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the CompositeValue factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeValueConfig {
    /// Weight for book-to-price (default: 0.5)
    pub book_to_price_weight: f64,
    /// Weight for earnings yield (default: 0.5)
    pub earnings_yield_weight: f64,
}

impl Default for CompositeValueConfig {
    fn default() -> Self {
        Self {
            book_to_price_weight: 0.5,
            earnings_yield_weight: 0.5,
        }
    }
}

/// CompositeValue computes a combined value signal from book-to-price and earnings yield
#[derive(Debug)]
pub struct CompositeValueFactor {
    config: CompositeValueConfig,
}

impl Factor for CompositeValueFactor {
    fn name(&self) -> &str {
        "composite_value"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Step 1: Compute individual value components
        // Book-to-price: book_value / market_cap
        let result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([when(col("market_cap").gt(lit(0.0)))
                .then(col("book_value") / col("market_cap"))
                .otherwise(lit(NULL))
                .alias("raw_b2p")])
            // Earnings yield: earnings / market_cap
            .with_columns([when(col("market_cap").gt(lit(0.0)))
                .then(col("earnings") / col("market_cap"))
                .otherwise(lit(NULL))
                .alias("raw_ey")])
            // Step 2: Standardize each component cross-sectionally using toraniko-math
            .with_columns([
                center_xsection("raw_b2p", "date", true).alias("std_b2p"),
                center_xsection("raw_ey", "date", true).alias("std_ey"),
            ])
            // Step 3: Weighted average based on config
            .with_columns([(col("std_b2p") * lit(self.config.book_to_price_weight)
                + col("std_ey") * lit(self.config.earnings_yield_weight))
            .alias("raw_composite")])
            // Step 4: Final cross-sectional standardization using toraniko-math
            .with_columns([center_xsection("raw_composite", "date", true).alias("composite_value_score")])
            .select([col("symbol"), col("date"), col("composite_value_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "book_value", "earnings", "market_cap"]
    }
}

impl StyleFactor for CompositeValueFactor {
    type Config = CompositeValueConfig;

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

impl Default for CompositeValueFactor {
    fn default() -> Self {
        Self::with_config(CompositeValueConfig::default())
    }
}
