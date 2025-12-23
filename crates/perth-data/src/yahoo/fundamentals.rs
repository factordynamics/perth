//! Fundamental data fetching from Yahoo Finance.

use crate::error::{DataError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

/// Company fundamental data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundamentalData {
    /// Stock symbol
    pub symbol: String,
    /// Market capitalization
    pub market_cap: Option<f64>,
    /// Enterprise value
    pub enterprise_value: Option<f64>,
    /// Trailing P/E ratio
    pub trailing_pe: Option<f64>,
    /// Forward P/E ratio
    pub forward_pe: Option<f64>,
    /// Price to book ratio
    pub price_to_book: Option<f64>,
    /// Price to sales ratio
    pub price_to_sales: Option<f64>,
    /// PEG ratio
    pub peg_ratio: Option<f64>,
    /// Book value per share
    pub book_value: Option<f64>,
    /// Dividend yield
    pub dividend_yield: Option<f64>,
    /// Beta
    pub beta: Option<f64>,
    /// 52-week high
    pub fifty_two_week_high: Option<f64>,
    /// 52-week low
    pub fifty_two_week_low: Option<f64>,
    /// 50-day moving average
    pub fifty_day_average: Option<f64>,
    /// 200-day moving average
    pub two_hundred_day_average: Option<f64>,
    /// Average volume (10 days)
    pub avg_volume_10d: Option<u64>,
    /// Shares outstanding
    pub shares_outstanding: Option<u64>,
    /// Float shares
    pub float_shares: Option<u64>,
    /// Held by insiders (%)
    pub held_percent_insiders: Option<f64>,
    /// Held by institutions (%)
    pub held_percent_institutions: Option<f64>,
    /// Short ratio
    pub short_ratio: Option<f64>,
    /// Revenue (TTM)
    pub revenue_ttm: Option<f64>,
    /// Net income (TTM)
    pub net_income_ttm: Option<f64>,
    /// Earnings per share (TTM)
    pub eps_ttm: Option<f64>,
    /// Return on equity
    pub return_on_equity: Option<f64>,
    /// Return on assets
    pub return_on_assets: Option<f64>,
    /// Debt to equity ratio
    pub debt_to_equity: Option<f64>,
    /// Current ratio
    pub current_ratio: Option<f64>,
    /// Operating cash flow (TTM)
    pub operating_cash_flow: Option<f64>,
    /// Free cash flow (TTM)
    pub free_cash_flow: Option<f64>,
}

/// Yahoo Finance fundamentals provider.
#[derive(Debug)]
pub struct YahooFundamentalsProvider {
    #[allow(dead_code)]
    client: reqwest::Client,
    rate_limit_delay: Duration,
}

impl YahooFundamentalsProvider {
    /// Create a new Yahoo Finance fundamentals provider.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
                .build()
                .expect("Failed to create HTTP client"),
            rate_limit_delay: Duration::from_millis(1000),
        }
    }

    /// Create a new provider with custom rate limiting.
    pub fn with_rate_limit(rate_limit_delay: Duration) -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
                .build()
                .expect("Failed to create HTTP client"),
            rate_limit_delay,
        }
    }

    /// Fetch fundamental data for a single symbol.
    ///
    /// Note: This is a placeholder implementation. In production, you would:
    /// 1. Use Yahoo Finance's statistics API
    /// 2. Parse the JSON response
    /// 3. Extract fundamental metrics
    ///
    /// For now, this returns mock data structure.
    pub async fn fetch_fundamentals(&self, symbol: &str) -> Result<FundamentalData> {
        if symbol.is_empty() {
            return Err(DataError::InvalidSymbol("Empty symbol".to_string()));
        }

        // Apply rate limiting
        sleep(self.rate_limit_delay).await;

        // Placeholder - in production, implement actual API calls
        // Example URL: https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=...

        Ok(FundamentalData {
            symbol: symbol.to_string(),
            market_cap: None,
            enterprise_value: None,
            trailing_pe: None,
            forward_pe: None,
            price_to_book: None,
            price_to_sales: None,
            peg_ratio: None,
            book_value: None,
            dividend_yield: None,
            beta: None,
            fifty_two_week_high: None,
            fifty_two_week_low: None,
            fifty_day_average: None,
            two_hundred_day_average: None,
            avg_volume_10d: None,
            shares_outstanding: None,
            float_shares: None,
            held_percent_insiders: None,
            held_percent_institutions: None,
            short_ratio: None,
            revenue_ttm: None,
            net_income_ttm: None,
            eps_ttm: None,
            return_on_equity: None,
            return_on_assets: None,
            debt_to_equity: None,
            current_ratio: None,
            operating_cash_flow: None,
            free_cash_flow: None,
        })
    }

    /// Fetch fundamental data for multiple symbols.
    pub async fn fetch_fundamentals_batch(
        &self,
        symbols: &[String],
    ) -> Result<Vec<FundamentalData>> {
        let mut fundamentals = Vec::new();

        for symbol in symbols {
            match self.fetch_fundamentals(symbol).await {
                Ok(data) => fundamentals.push(data),
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to fetch fundamentals for {}: {}",
                        symbol, e
                    );
                    continue;
                }
            }
        }

        Ok(fundamentals)
    }

    /// Convert fundamental data to Polars DataFrame.
    pub fn to_dataframe(data: Vec<FundamentalData>) -> Result<DataFrame> {
        if data.is_empty() {
            return Err(DataError::MissingData {
                symbol: "batch".to_string(),
                reason: "No fundamental data provided".to_string(),
            });
        }

        let symbols: Vec<String> = data.iter().map(|d| d.symbol.clone()).collect();
        let market_caps: Vec<Option<f64>> = data.iter().map(|d| d.market_cap).collect();
        let trailing_pes: Vec<Option<f64>> = data.iter().map(|d| d.trailing_pe).collect();
        let betas: Vec<Option<f64>> = data.iter().map(|d| d.beta).collect();
        let book_values: Vec<Option<f64>> = data.iter().map(|d| d.book_value).collect();

        let df = DataFrame::new(vec![
            Series::new("symbol".into(), symbols).into(),
            Series::new("market_cap".into(), market_caps).into(),
            Series::new("trailing_pe".into(), trailing_pes).into(),
            Series::new("beta".into(), betas).into(),
            Series::new("book_value".into(), book_values).into(),
        ])?;

        Ok(df)
    }
}

impl Default for YahooFundamentalsProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_fundamentals() {
        let provider = YahooFundamentalsProvider::new();
        let result = provider.fetch_fundamentals("AAPL").await;
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.symbol, "AAPL");
    }

    #[tokio::test]
    async fn test_invalid_symbol() {
        let provider = YahooFundamentalsProvider::new();
        let result = provider.fetch_fundamentals("").await;
        assert!(matches!(result, Err(DataError::InvalidSymbol(_))));
    }

    #[test]
    fn test_to_dataframe() {
        let data = vec![FundamentalData {
            symbol: "AAPL".to_string(),
            market_cap: Some(3_000_000_000_000.0),
            trailing_pe: Some(30.0),
            beta: Some(1.2),
            book_value: Some(4.0),
            enterprise_value: None,
            forward_pe: None,
            price_to_book: None,
            price_to_sales: None,
            peg_ratio: None,
            dividend_yield: None,
            fifty_two_week_high: None,
            fifty_two_week_low: None,
            fifty_day_average: None,
            two_hundred_day_average: None,
            avg_volume_10d: None,
            shares_outstanding: None,
            float_shares: None,
            held_percent_insiders: None,
            held_percent_institutions: None,
            short_ratio: None,
            revenue_ttm: None,
            net_income_ttm: None,
            eps_ttm: None,
            return_on_equity: None,
            return_on_assets: None,
            debt_to_equity: None,
            current_ratio: None,
            operating_cash_flow: None,
            free_cash_flow: None,
        }];

        let result = YahooFundamentalsProvider::to_dataframe(data);
        assert!(result.is_ok());

        let df = result.unwrap();
        assert_eq!(df.height(), 1);
    }
}
