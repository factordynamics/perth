//! Log Market Capitalization Factor
//!
//! Measures company size using the natural logarithm of market capitalization.
//! Negative scores indicate smaller companies, positive scores indicate larger companies.
//! The size premium suggests smaller companies tend to outperform.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

/// Configuration for the LogMarketCap factor
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LogMarketCapConfig {
    /// Minimum market cap to include (default: None)
    pub min_market_cap: Option<f64>,
}

/// LogMarketCap computes company size using natural logarithm of market capitalization
#[derive(Debug)]
pub struct LogMarketCapFactor {
    config: LogMarketCapConfig,
}

impl Factor for LogMarketCapFactor {
    fn name(&self) -> &str {
        "log_market_cap"
    }

    fn kind(&self) -> FactorKind {
        FactorKind::Style
    }

    fn compute_scores(&self, data: LazyFrame) -> Result<LazyFrame, FactorError> {
        // 1. Filter by minimum market cap if configured
        let mut result = data;

        if let Some(min_cap) = self.config.min_market_cap {
            result = result.filter(col("market_cap").gt_eq(lit(min_cap)));
        }

        // 2. Compute ln(market_cap) using natural logarithm
        // Polars doesn't have direct log methods, so we use apply with a custom function
        result = result.with_columns([col("market_cap")
            .apply(
                |c: Column| {
                    let s = c.as_materialized_series();
                    Ok(Some(s.f64()?.apply_values(|v| v.ln()).into_series().into()))
                },
                GetOutput::from_type(DataType::Float64),
            )
            .alias("raw_log_market_cap")]);

        // 3. Cross-sectionally standardize (mean=0, std=1) by date
        result = result
            .with_columns([
                col("raw_log_market_cap")
                    .mean()
                    .over([col("date")])
                    .alias("log_market_cap_mean"),
                col("raw_log_market_cap")
                    .std(1)
                    .over([col("date")])
                    .alias("log_market_cap_std"),
            ])
            .with_columns([((col("raw_log_market_cap") - col("log_market_cap_mean"))
                / col("log_market_cap_std"))
            .alias("size_score")]);

        // 4. Return LazyFrame with symbol, date, and size_score columns
        Ok(result.select([col("symbol"), col("date"), col("size_score")]))
    }

    fn required_columns(&self) -> &[&str] {
        &["symbol", "date", "market_cap"]
    }
}

impl StyleFactor for LogMarketCapFactor {
    type Config = LogMarketCapConfig;

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

impl Default for LogMarketCapFactor {
    fn default() -> Self {
        Self::with_config(LogMarketCapConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factor_name() {
        let factor = LogMarketCapFactor::default();
        assert_eq!(factor.name(), "log_market_cap");
        assert_eq!(factor.kind(), FactorKind::Style);
    }

    #[test]
    fn test_required_columns() {
        let factor = LogMarketCapFactor::default();
        let cols = factor.required_columns();
        assert_eq!(cols.len(), 3);
        assert!(cols.contains(&"symbol"));
        assert!(cols.contains(&"date"));
        assert!(cols.contains(&"market_cap"));
    }

    #[test]
    fn test_config_defaults() {
        let config = LogMarketCapConfig::default();
        assert_eq!(config.min_market_cap, None);
    }

    #[test]
    fn test_custom_config() {
        let config = LogMarketCapConfig {
            min_market_cap: Some(1_000_000.0),
        };
        let factor = LogMarketCapFactor::with_config(config);
        assert_eq!(factor.config().min_market_cap, Some(1_000_000.0));
    }

    #[test]
    fn test_residualize() {
        let factor = LogMarketCapFactor::default();
        assert!(factor.residualize());
    }
}
