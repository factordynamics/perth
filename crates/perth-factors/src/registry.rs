//! Factor Registry
//!
//! Central registry for all available factors. Allows dynamic factor lookup
//! and instantiation by name.

use std::collections::HashMap;

/// Available factor categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FactorCategory {
    /// Value factors (book-to-price, earnings yield)
    Value,
    /// Momentum factors (short, medium, long-term)
    Momentum,
    /// Size factors (market capitalization)
    Size,
    /// Volatility factors (beta, historical volatility)
    Volatility,
    /// Quality factors (ROE, leverage)
    Quality,
    /// Growth factors (earnings growth, sales growth)
    Growth,
    /// Liquidity factors (turnover, Amihud illiquidity)
    Liquidity,
}

/// Factor metadata
#[derive(Debug, Clone)]
pub struct FactorInfo {
    /// Factor name (unique identifier)
    pub name: &'static str,
    /// Factor category
    pub category: FactorCategory,
    /// Brief description of what the factor measures
    pub description: &'static str,
    /// Required column names in input data
    pub required_columns: &'static [&'static str],
}

/// Get all available factor info
pub fn available_factors() -> Vec<FactorInfo> {
    vec![
        // Value factors
        FactorInfo {
            name: "book_to_price",
            category: FactorCategory::Value,
            description: "Book value to market price ratio",
            required_columns: &["symbol", "date", "book_value", "market_cap"],
        },
        FactorInfo {
            name: "earnings_yield",
            category: FactorCategory::Value,
            description: "Earnings to market price ratio (inverse of P/E)",
            required_columns: &["symbol", "date", "earnings", "market_cap"],
        },
        // Momentum factors
        FactorInfo {
            name: "short_term_momentum",
            category: FactorCategory::Momentum,
            description: "Short-term price momentum (1 month)",
            required_columns: &["symbol", "date", "price", "returns"],
        },
        FactorInfo {
            name: "medium_term_momentum",
            category: FactorCategory::Momentum,
            description: "Medium-term price momentum (6 months)",
            required_columns: &["symbol", "date", "price", "returns"],
        },
        FactorInfo {
            name: "long_term_momentum",
            category: FactorCategory::Momentum,
            description: "Long-term price momentum (12 months)",
            required_columns: &["symbol", "date", "price", "returns"],
        },
        // Size factors
        FactorInfo {
            name: "log_market_cap",
            category: FactorCategory::Size,
            description: "Natural logarithm of market capitalization",
            required_columns: &["symbol", "date", "market_cap"],
        },
        // Volatility factors
        FactorInfo {
            name: "beta",
            category: FactorCategory::Volatility,
            description: "Market beta - systematic risk exposure",
            required_columns: &["symbol", "date", "returns", "market_return"],
        },
        FactorInfo {
            name: "historical_volatility",
            category: FactorCategory::Volatility,
            description: "Realized volatility of returns",
            required_columns: &["symbol", "date", "returns"],
        },
        // Quality factors
        FactorInfo {
            name: "roe",
            category: FactorCategory::Quality,
            description: "Return on equity - profitability measure",
            required_columns: &["symbol", "date", "net_income", "shareholders_equity"],
        },
        FactorInfo {
            name: "leverage",
            category: FactorCategory::Quality,
            description: "Financial leverage - debt-to-equity ratio",
            required_columns: &["symbol", "date", "total_debt", "shareholders_equity"],
        },
        // Growth factors
        FactorInfo {
            name: "earnings_growth",
            category: FactorCategory::Growth,
            description: "Year-over-year earnings growth",
            required_columns: &["symbol", "date", "earnings"],
        },
        FactorInfo {
            name: "sales_growth",
            category: FactorCategory::Growth,
            description: "Year-over-year sales/revenue growth",
            required_columns: &["symbol", "date", "sales"],
        },
        // Liquidity factors
        FactorInfo {
            name: "turnover",
            category: FactorCategory::Liquidity,
            description: "Trading volume relative to shares outstanding",
            required_columns: &["symbol", "date", "volume", "shares_outstanding"],
        },
        FactorInfo {
            name: "amihud",
            category: FactorCategory::Liquidity,
            description: "Amihud illiquidity - price impact per unit volume",
            required_columns: &["symbol", "date", "returns", "price", "volume"],
        },
    ]
}

/// Get factors by category
pub fn factors_by_category(category: FactorCategory) -> Vec<FactorInfo> {
    available_factors()
        .into_iter()
        .filter(|f| f.category == category)
        .collect()
}

/// Get factor info by name
pub fn get_factor_info(name: &str) -> Option<FactorInfo> {
    available_factors().into_iter().find(|f| f.name == name)
}

/// Get a map of all factors indexed by name
pub fn factor_map() -> HashMap<&'static str, FactorInfo> {
    available_factors()
        .into_iter()
        .map(|f| (f.name, f))
        .collect()
}

/// List all factor names
pub fn list_factor_names() -> Vec<&'static str> {
    available_factors().into_iter().map(|f| f.name).collect()
}

/// Count factors by category
pub fn count_by_category() -> HashMap<FactorCategory, usize> {
    let mut counts = HashMap::new();
    for factor in available_factors() {
        *counts.entry(factor.category).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_factors_count() {
        let factors = available_factors();
        // We have 14 individual factors
        assert_eq!(factors.len(), 14);
    }

    #[test]
    fn test_factors_by_category() {
        let value_factors = factors_by_category(FactorCategory::Value);
        assert_eq!(value_factors.len(), 2);

        let momentum_factors = factors_by_category(FactorCategory::Momentum);
        assert_eq!(momentum_factors.len(), 3);

        let size_factors = factors_by_category(FactorCategory::Size);
        assert_eq!(size_factors.len(), 1);

        let volatility_factors = factors_by_category(FactorCategory::Volatility);
        assert_eq!(volatility_factors.len(), 2);

        let quality_factors = factors_by_category(FactorCategory::Quality);
        assert_eq!(quality_factors.len(), 2);

        let growth_factors = factors_by_category(FactorCategory::Growth);
        assert_eq!(growth_factors.len(), 2);

        let liquidity_factors = factors_by_category(FactorCategory::Liquidity);
        assert_eq!(liquidity_factors.len(), 2);
    }

    #[test]
    fn test_get_factor_info() {
        let beta_info = get_factor_info("beta");
        assert!(beta_info.is_some());
        let beta = beta_info.unwrap();
        assert_eq!(beta.name, "beta");
        assert_eq!(beta.category, FactorCategory::Volatility);
        assert!(beta.required_columns.contains(&"returns"));

        let nonexistent = get_factor_info("nonexistent_factor");
        assert!(nonexistent.is_none());
    }

    #[test]
    fn test_factor_map() {
        let map = factor_map();
        assert_eq!(map.len(), 14);
        assert!(map.contains_key("beta"));
        assert!(map.contains_key("log_market_cap"));
        assert!(map.contains_key("roe"));
    }

    #[test]
    fn test_list_factor_names() {
        let names = list_factor_names();
        assert_eq!(names.len(), 14);
        assert!(names.contains(&"beta"));
        assert!(names.contains(&"log_market_cap"));
        assert!(names.contains(&"earnings_growth"));
    }

    #[test]
    fn test_count_by_category() {
        let counts = count_by_category();
        assert_eq!(counts.get(&FactorCategory::Value), Some(&2));
        assert_eq!(counts.get(&FactorCategory::Momentum), Some(&3));
        assert_eq!(counts.get(&FactorCategory::Size), Some(&1));
        assert_eq!(counts.get(&FactorCategory::Volatility), Some(&2));
        assert_eq!(counts.get(&FactorCategory::Quality), Some(&2));
        assert_eq!(counts.get(&FactorCategory::Growth), Some(&2));
        assert_eq!(counts.get(&FactorCategory::Liquidity), Some(&2));
    }

    #[test]
    fn test_all_factors_have_required_columns() {
        for factor in available_factors() {
            assert!(
                !factor.required_columns.is_empty(),
                "Factor {} has no required columns",
                factor.name
            );
            assert!(
                factor.required_columns.contains(&"symbol"),
                "Factor {} missing 'symbol' in required columns",
                factor.name
            );
            assert!(
                factor.required_columns.contains(&"date"),
                "Factor {} missing 'date' in required columns",
                factor.name
            );
        }
    }
}
