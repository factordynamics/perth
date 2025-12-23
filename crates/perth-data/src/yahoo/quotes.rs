//! Quote data fetching from Yahoo Finance.

use crate::error::{DataError, Result};
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::time::Duration;
use tokio::time::sleep;
use yahoo_finance_api as yahoo;

/// Yahoo Finance quote provider with rate limiting.
pub struct YahooQuoteProvider {
    provider: yahoo::YahooConnector,
    rate_limit_delay: Duration,
}

impl std::fmt::Debug for YahooQuoteProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YahooQuoteProvider")
            .field("rate_limit_delay", &self.rate_limit_delay)
            .finish_non_exhaustive()
    }
}

impl YahooQuoteProvider {
    /// Create a new Yahoo Finance quote provider with default rate limiting (1 req/sec).
    pub fn new() -> Self {
        Self {
            provider: yahoo::YahooConnector::new().expect("Failed to create Yahoo connector"),
            rate_limit_delay: Duration::from_millis(1000),
        }
    }

    /// Create a new Yahoo Finance quote provider with custom rate limiting.
    pub fn with_rate_limit(rate_limit_delay: Duration) -> Self {
        Self {
            provider: yahoo::YahooConnector::new().expect("Failed to create Yahoo connector"),
            rate_limit_delay,
        }
    }

    /// Fetch OHLCV data for a single symbol.
    ///
    /// # Arguments
    /// * `symbol` - The ticker symbol (e.g., "AAPL")
    /// * `start` - Start date for the data
    /// * `end` - End date for the data
    ///
    /// # Returns
    /// A Polars DataFrame with columns: date, open, high, low, close, volume, adjusted_close
    pub async fn fetch_quotes(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<DataFrame> {
        // Validate date range
        if start > end {
            return Err(DataError::InvalidDateRange {
                start: start.to_rfc3339(),
                end: end.to_rfc3339(),
            });
        }

        // Validate symbol
        if symbol.is_empty() {
            return Err(DataError::InvalidSymbol("Empty symbol".to_string()));
        }

        // Convert chrono DateTime to time::OffsetDateTime
        let start_time = time::OffsetDateTime::from_unix_timestamp(start.timestamp())
            .map_err(|e| DataError::TimeConversion(e.to_string()))?;
        let end_time = time::OffsetDateTime::from_unix_timestamp(end.timestamp())
            .map_err(|e| DataError::TimeConversion(e.to_string()))?;

        // Fetch data from Yahoo Finance
        let response = self
            .provider
            .get_quote_history(symbol, start_time, end_time)
            .await?;

        let quotes = response
            .quotes()
            .map_err(|e| DataError::YahooApi(e.to_string()))?;

        if quotes.is_empty() {
            return Err(DataError::MissingData {
                symbol: symbol.to_string(),
                reason: "No data returned from Yahoo Finance".to_string(),
            });
        }

        // Convert to DataFrame
        let dates: Vec<i64> = quotes.iter().map(|q| q.timestamp).collect();
        let opens: Vec<f64> = quotes.iter().map(|q| q.open).collect();
        let highs: Vec<f64> = quotes.iter().map(|q| q.high).collect();
        let lows: Vec<f64> = quotes.iter().map(|q| q.low).collect();
        let closes: Vec<f64> = quotes.iter().map(|q| q.close).collect();
        let volumes: Vec<u64> = quotes.iter().map(|q| q.volume).collect();
        let adj_closes: Vec<f64> = quotes.iter().map(|q| q.adjclose).collect();

        let mut df = DataFrame::new(vec![
            Series::new("timestamp".into(), dates).into(),
            Series::new("open".into(), opens).into(),
            Series::new("high".into(), highs).into(),
            Series::new("low".into(), lows).into(),
            Series::new("close".into(), closes).into(),
            Series::new("volume".into(), volumes).into(),
            Series::new("adjusted_close".into(), adj_closes).into(),
        ])?;

        // Add symbol column
        let symbol_col: Column = Series::new("symbol".into(), vec![symbol; df.height()]).into();
        df.with_column(symbol_col)?;

        // Convert timestamp to date
        let df = df
            .lazy()
            .with_column(
                (col("timestamp") * lit(1_000_000_000))
                    .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                    .cast(DataType::Date)
                    .alias("date"),
            )
            .select(&[
                col("symbol"),
                col("date"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
                col("adjusted_close"),
            ])
            .collect()?;

        // Apply rate limiting
        sleep(self.rate_limit_delay).await;

        Ok(df)
    }

    /// Fetch OHLCV data for multiple symbols.
    ///
    /// # Arguments
    /// * `symbols` - List of ticker symbols
    /// * `start` - Start date for the data
    /// * `end` - End date for the data
    ///
    /// # Returns
    /// A Polars DataFrame with all symbols combined
    pub async fn fetch_quotes_batch(
        &self,
        symbols: &[String],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<DataFrame> {
        let mut dfs = Vec::new();

        for symbol in symbols {
            match self.fetch_quotes(symbol, start, end).await {
                Ok(df) => dfs.push(df.lazy()),
                Err(e) => {
                    eprintln!("Warning: Failed to fetch data for {}: {}", symbol, e);
                    continue;
                }
            }
        }

        if dfs.is_empty() {
            return Err(DataError::MissingData {
                symbol: "batch".to_string(),
                reason: "No data fetched for any symbol".to_string(),
            });
        }

        // Concatenate all dataframes
        let combined = concat(dfs, UnionArgs::default())?.collect()?;

        Ok(combined)
    }
}

impl Default for YahooQuoteProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    #[tokio::test]
    async fn test_fetch_quotes() {
        let provider = YahooQuoteProvider::new();
        let end = Utc::now();
        let start = end - ChronoDuration::days(30);

        let result = provider.fetch_quotes("AAPL", start, end).await;
        assert!(result.is_ok());

        let df = result.unwrap();
        assert!(df.height() > 0);
        assert_eq!(
            df.get_column_names(),
            vec![
                "symbol",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "adjusted_close"
            ]
        );
    }

    #[tokio::test]
    async fn test_invalid_date_range() {
        let provider = YahooQuoteProvider::new();
        let start = Utc::now();
        let end = start - ChronoDuration::days(30);

        let result = provider.fetch_quotes("AAPL", start, end).await;
        assert!(matches!(result, Err(DataError::InvalidDateRange { .. })));
    }

    #[tokio::test]
    async fn test_invalid_symbol() {
        let provider = YahooQuoteProvider::new();
        let end = Utc::now();
        let start = end - ChronoDuration::days(30);

        let result = provider.fetch_quotes("", start, end).await;
        assert!(matches!(result, Err(DataError::InvalidSymbol(_))));
    }
}
