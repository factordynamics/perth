//! Data pipeline for fetching and preparing universe data.
//!
//! Provides functions to fetch OHLCV data for the S&P 500 universe,
//! compute returns, and prepare market cap data for factor model estimation.
//! Supports caching via SQLite to avoid repeated Yahoo Finance API calls.

use super::cache_manager;
use chrono::{DateTime, NaiveDate, Utc};
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use perth::universe::SP500Universe;
use perth_data::yahoo::quotes::YahooQuoteProvider;
use polars::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Error type for data pipeline operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DataPipelineError {
    /// Data fetch error from Yahoo.
    #[error("Data fetch error: {0}")]
    Fetch(#[from] perth_data::error::DataError),
    /// Polars DataFrame error.
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
}

/// Configuration for data fetching.
#[derive(Debug, Clone)]
pub(crate) struct FetchConfig {
    /// Whether to use the cache.
    pub use_cache: bool,
    /// Whether to force refresh (ignore cache).
    pub force_refresh: bool,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            use_cache: true,
            force_refresh: false,
        }
    }
}

/// Convert DateTime<Utc> to NaiveDate for cache lookups.
fn to_naive_date(dt: DateTime<Utc>) -> NaiveDate {
    dt.date_naive()
}

/// Fetch OHLCV data for all symbols in the universe.
///
/// Uses caching by default: checks SQLite cache first, then fetches missing
/// data from Yahoo Finance and stores it in the cache.
#[allow(dead_code)]
pub(crate) async fn fetch_universe_data(
    provider: &YahooQuoteProvider,
    universe: &SP500Universe,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<DataFrame, DataPipelineError> {
    fetch_universe_data_with_config(provider, universe, start, end, FetchConfig::default()).await
}

/// Fetch OHLCV data for all symbols with custom configuration.
pub(crate) async fn fetch_universe_data_with_config(
    provider: &YahooQuoteProvider,
    universe: &SP500Universe,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: FetchConfig,
) -> Result<DataFrame, DataPipelineError> {
    fetch_universe_data_with_progress(provider, universe, start, end, config, None).await
}

/// Default number of concurrent fetches.
const DEFAULT_CONCURRENCY: usize = 10;

/// Fetch OHLCV data for all symbols with custom configuration and optional progress bar.
pub(crate) async fn fetch_universe_data_with_progress(
    provider: &YahooQuoteProvider,
    universe: &SP500Universe,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: FetchConfig,
    progress: Option<&ProgressBar>,
) -> Result<DataFrame, DataPipelineError> {
    let symbols = universe.symbols();
    let start_date = to_naive_date(start);
    let end_date = to_naive_date(end);

    // Try to open cache if enabled
    let cache = if config.use_cache {
        cache_manager::open_cache().ok()
    } else {
        None
    };

    let mut cached_dfs = Vec::new();
    let mut symbols_to_fetch = Vec::new();

    // Check cache for each symbol
    if let Some(ref cache) = cache {
        if !config.force_refresh {
            for symbol in &symbols {
                if cache
                    .has_quotes(symbol, start_date, end_date)
                    .unwrap_or(false)
                {
                    // Try to get cached data
                    if let Ok(df) = cache.get_quotes(symbol, start_date, end_date) {
                        cached_dfs.push(df.lazy());
                        continue;
                    }
                }
                symbols_to_fetch.push(symbol.clone());
            }
        } else {
            symbols_to_fetch = symbols;
        }
    } else {
        symbols_to_fetch = symbols;
    }

    // Update progress bar length based on what we actually need to fetch
    if let Some(pb) = progress {
        let total = cached_dfs.len() + symbols_to_fetch.len();
        pb.set_length(total as u64);
        // Mark cached symbols as already done
        pb.set_position(cached_dfs.len() as u64);
        if symbols_to_fetch.is_empty() {
            pb.set_message("Loading from cache...");
        } else {
            pb.set_message(format!(
                "Fetching {} symbols ({} concurrent)...",
                symbols_to_fetch.len(),
                DEFAULT_CONCURRENCY
            ));
        }
    }

    // Fetch missing data from Yahoo in parallel
    let fetched_dfs = if !symbols_to_fetch.is_empty() {
        // Use Arc<Mutex<>> for thread-safe collection of results
        let results: Arc<Mutex<Vec<LazyFrame>>> = Arc::new(Mutex::new(Vec::new()));
        let cache_arc = Arc::new(Mutex::new(cache));

        stream::iter(symbols_to_fetch)
            .map(|symbol| {
                let results = Arc::clone(&results);
                let cache = Arc::clone(&cache_arc);
                async move {
                    match provider.fetch_quotes(&symbol, start, end).await {
                        Ok(df) => {
                            // Store in cache if available
                            let cache_guard = cache.lock().await;
                            if let Some(ref cache) = *cache_guard
                                && let Err(e) = cache.put_quotes(&df)
                            {
                                eprintln!("Warning: Failed to cache quotes for {}: {}", symbol, e);
                            }
                            drop(cache_guard);
                            results.lock().await.push(df.lazy());
                            Ok(symbol)
                        }
                        Err(e) => Err((symbol, e)),
                    }
                }
            })
            .buffer_unordered(DEFAULT_CONCURRENCY)
            .for_each(|result| async {
                match result {
                    Ok(_symbol) => {
                        if let Some(pb) = progress {
                            pb.inc(1);
                        }
                    }
                    Err((symbol, e)) => {
                        if let Some(pb) = progress {
                            pb.suspend(|| {
                                eprintln!("Warning: Failed to fetch data for {}: {}", symbol, e);
                            });
                            pb.inc(1);
                        } else {
                            eprintln!("Warning: Failed to fetch data for {}: {}", symbol, e);
                        }
                    }
                }
            })
            .await;

        Arc::try_unwrap(results).map_or_else(
            |_| unreachable!("all tasks completed, Arc should have single owner"),
            |mutex| mutex.into_inner(),
        )
    } else {
        Vec::new()
    };

    // Combine cached and fetched data
    let all_dfs: Vec<_> = cached_dfs.into_iter().chain(fetched_dfs).collect();

    if all_dfs.is_empty() {
        return Err(DataPipelineError::Fetch(
            perth_data::error::DataError::MissingData {
                symbol: "batch".to_string(),
                reason: "No data fetched for any symbol".to_string(),
            },
        ));
    }

    // Concatenate all dataframes
    let combined = concat(all_dfs, UnionArgs::default())?.collect()?;

    Ok(combined)
}

/// Fetch a single symbol's data with caching support.
pub(crate) async fn fetch_symbol_data(
    provider: &YahooQuoteProvider,
    symbol: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: &FetchConfig,
) -> Result<DataFrame, DataPipelineError> {
    let start_date = to_naive_date(start);
    let end_date = to_naive_date(end);

    // Try cache first if enabled
    if config.use_cache
        && !config.force_refresh
        && let Ok(cache) = cache_manager::open_cache()
        && cache
            .has_quotes(symbol, start_date, end_date)
            .unwrap_or(false)
        && let Ok(df) = cache.get_quotes(symbol, start_date, end_date)
    {
        return Ok(df);
    }

    // Fetch from Yahoo
    let df = provider.fetch_quotes(symbol, start, end).await?;

    // Cache the result
    if config.use_cache
        && let Ok(cache) = cache_manager::open_cache()
        && let Err(e) = cache.put_quotes(&df)
    {
        eprintln!("Warning: Failed to cache quotes for {}: {}", symbol, e);
    }

    Ok(df)
}

/// Compute daily returns from adjusted close prices.
///
/// Returns a LazyFrame with columns: [date, symbol, asset_returns]
pub(crate) fn compute_returns(quotes: &DataFrame) -> Result<LazyFrame, DataPipelineError> {
    let returns = quotes
        .clone()
        .lazy()
        .sort(["symbol", "date"], SortMultipleOptions::default())
        .with_column(
            (col("adjusted_close") / col("adjusted_close").shift(lit(1)).over([col("symbol")])
                - lit(1.0))
            .alias("asset_returns"),
        )
        .filter(col("asset_returns").is_not_null())
        .select([col("date"), col("symbol"), col("asset_returns")]);

    Ok(returns)
}

/// Compute market cap proxy using volume * close.
///
/// Since Yahoo doesn't provide shares outstanding consistently,
/// we use trading volume * close price as a proxy for market cap.
/// This is sufficient for relative weighting in WLS regression.
///
/// Returns a LazyFrame with columns: [date, symbol, market_cap]
pub(crate) fn compute_market_cap_proxy(quotes: &DataFrame) -> Result<LazyFrame, DataPipelineError> {
    let mkt_cap = quotes
        .clone()
        .lazy()
        .with_column((col("volume").cast(DataType::Float64) * col("close")).alias("market_cap"))
        .select([col("date"), col("symbol"), col("market_cap")]);

    Ok(mkt_cap)
}

/// Fetch market benchmark (SPY) returns with caching support.
///
/// Returns a LazyFrame with columns: [date, market_return]
#[allow(dead_code)]
pub(crate) async fn fetch_market_benchmark(
    provider: &YahooQuoteProvider,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<LazyFrame, DataPipelineError> {
    fetch_market_benchmark_with_config(provider, start, end, FetchConfig::default()).await
}

/// Fetch market benchmark (SPY) returns with custom configuration.
pub(crate) async fn fetch_market_benchmark_with_config(
    provider: &YahooQuoteProvider,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: FetchConfig,
) -> Result<LazyFrame, DataPipelineError> {
    let spy_quotes = fetch_symbol_data(provider, "SPY", start, end, &config).await?;

    let spy_returns = spy_quotes
        .lazy()
        .sort(["date"], SortMultipleOptions::default())
        .with_column(
            (col("adjusted_close") / col("adjusted_close").shift(lit(1)) - lit(1.0))
                .alias("market_return"),
        )
        .filter(col("market_return").is_not_null())
        .select([col("date"), col("market_return")]);

    Ok(spy_returns)
}

/// Prepare combined data for factor computation.
///
/// Joins quotes with market returns and prepares all columns needed
/// for factor score computation.
///
/// Returns DataFrame with columns:
/// [date, symbol, adjusted_close, close, volume, asset_returns, market_return, market_cap]
pub(crate) fn prepare_factor_data(
    quotes: &DataFrame,
    market_returns: &LazyFrame,
    market_cap: &LazyFrame,
) -> Result<DataFrame, DataPipelineError> {
    let returns = compute_returns(quotes)?;

    let combined = quotes
        .clone()
        .lazy()
        // Join with returns
        .join(
            returns,
            [col("date"), col("symbol")],
            [col("date"), col("symbol")],
            JoinArgs::new(JoinType::Inner),
        )
        // Join with market returns
        .join(
            market_returns.clone(),
            [col("date")],
            [col("date")],
            JoinArgs::new(JoinType::Inner),
        )
        // Join with market cap
        .join(
            market_cap.clone(),
            [col("date"), col("symbol")],
            [col("date"), col("symbol")],
            JoinArgs::new(JoinType::Inner),
        )
        .select([
            col("date"),
            col("symbol"),
            col("adjusted_close"),
            col("close"),
            col("volume"),
            col("asset_returns"),
            col("market_return"),
            col("market_cap"),
        ])
        .collect()?;

    Ok(combined)
}

/// Get cache statistics if cache is available.
pub(crate) fn get_cache_stats() -> Option<(usize, usize)> {
    cache_manager::open_cache()
        .ok()
        .and_then(|cache| cache.get_stats().ok())
        .map(|stats| (stats.total_quotes, stats.unique_symbols))
}

/// Print cache location info.
pub(crate) fn print_cache_info() {
    let path = cache_manager::get_cache_path();
    println!("  Cache location: {}", path.display());
    if let Some((quotes, symbols)) = get_cache_stats() {
        println!("  Cached data: {} quotes for {} symbols", quotes, symbols);
    }
}
