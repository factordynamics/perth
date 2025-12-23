//! Book-to-Price Factor
//!
//! Measures the ratio of book value to market price. Higher values indicate
//! potentially undervalued securities based on fundamental book value.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
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

        // Step 2: Winsorize if configured
        if self.config.winsorize {
            let lower_pct = self.config.winsorize_pct;
            let upper_pct = 1.0 - self.config.winsorize_pct;

            result = result
                .with_columns([
                    col("raw_b2p")
                        .quantile(lit(lower_pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("b2p_lower"),
                    col("raw_b2p")
                        .quantile(lit(upper_pct), QuantileMethod::Linear)
                        .over([col("date")])
                        .alias("b2p_upper"),
                ])
                .with_columns([when(col("raw_b2p").lt(col("b2p_lower")))
                    .then(col("b2p_lower"))
                    .when(col("raw_b2p").gt(col("b2p_upper")))
                    .then(col("b2p_upper"))
                    .otherwise(col("raw_b2p"))
                    .alias("winsorized_b2p")]);
        } else {
            result = result.with_columns([col("raw_b2p").alias("winsorized_b2p")]);
        }

        // Step 3: Cross-sectional standardization (mean=0, std=1) by date
        result = result
            .with_columns([
                col("winsorized_b2p")
                    .mean()
                    .over([col("date")])
                    .alias("b2p_mean"),
                col("winsorized_b2p")
                    .std(1)
                    .over([col("date")])
                    .alias("b2p_std"),
            ])
            .with_columns(
                [((col("winsorized_b2p") - col("b2p_mean")) / col("b2p_std"))
                    .alias("book_to_price_score")],
            )
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
