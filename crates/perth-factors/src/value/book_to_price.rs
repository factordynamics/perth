//! Book-to-Price Factor
//!
//! Measures the ratio of book value to market price. Higher values indicate
//! potentially undervalued securities based on fundamental book value.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_math::{center_xsection, winsorize_xsection};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the BookToPrice factor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookToPriceConfig {
    /// Whether to winsorize extreme values (default: true)
    pub winsorize: bool,
    /// Winsorization percentile (default: 0.01 for 1%/99%)
    pub winsorize_pct: f64,
}

impl Default for BookToPriceConfig {
    fn default() -> Self {
        Self {
            winsorize: true,
            winsorize_pct: 0.01,
        }
    }
}

/// BookToPrice computes the ratio of book value to market price
#[derive(Debug)]
pub struct BookToPriceFactor {
    config: BookToPriceConfig,
}

impl Factor for BookToPriceFactor {
    fn name(&self) -> &str {
        "book_to_price"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // Step 1: Compute book_value / market_cap
        // Handle edge cases: zero or negative market_cap -> null
        let mut result = data
            .sort(["symbol", "date"], Default::default())
            .with_columns([when(col("market_cap").gt(lit(0.0)))
                .then(col("book_value") / col("market_cap"))
                .otherwise(lit(NULL))
                .alias("raw_b2p")]);

        // Step 2: Winsorize if configured using toraniko-math
        if self.config.winsorize {
            result = winsorize_xsection(result, &["raw_b2p"], "date", self.config.winsorize_pct);
        }

        // Step 3: Cross-sectional standardization (mean=0, std=1) by date using toraniko-math
        let result = result
            .with_columns([center_xsection("raw_b2p", "date", true).alias("book_to_price_score")])
            .select([col("symbol"), col("date"), col("book_to_price_score")]);

        Ok(result)
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "book_value", "market_cap"]
    }
}

impl StyleFactor for BookToPriceFactor {
    type Config = BookToPriceConfig;

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

impl Default for BookToPriceFactor {
    fn default() -> Self {
        Self::with_config(BookToPriceConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_name() {
        let factor = BookToPriceFactor::default();
        assert_eq!(factor.name(), "book_to_price");
    }

    #[test]
    fn test_required_columns() {
        let factor = BookToPriceFactor::default();
        let cols = factor.required_columns();
        assert!(cols.contains(&"book_value"));
        assert!(cols.contains(&"market_cap"));
    }
}
