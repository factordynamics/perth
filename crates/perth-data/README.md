# perth-data

Data fetching and caching for the Perth institutional-grade factor model.

## Features

- **Yahoo Finance Integration**: Fetch OHLCV data and fundamental metrics
- **SQLite Caching**: Efficient local storage with automatic cache management
- **Polars DataFrames**: High-performance data manipulation
- **Rate Limiting**: Built-in rate limiting to respect API limits (1 req/sec default)
- **Async/Await**: Fully asynchronous API using Tokio
- **Error Handling**: Comprehensive error types using thiserror

## Architecture

### Modules

- `yahoo`: Yahoo Finance data providers
  - `quotes`: OHLCV historical data
  - `fundamentals`: Company fundamental metrics (placeholder)
- `cache`: SQLite caching layer
  - `sqlite`: Database operations for quotes, fundamentals, universe, and market caps
- `error`: Error types and Result aliases

## Usage

### Fetching Quote Data

```rust
use perth_data::yahoo::YahooQuoteProvider;
use chrono::{Utc, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = YahooQuoteProvider::new();
    let end = Utc::now();
    let start = end - Duration::days(30);

    // Fetch single symbol
    let quotes = provider.fetch_quotes("AAPL", start, end).await?;
    println!("Fetched {} rows for AAPL", quotes.height());

    // Fetch multiple symbols
    let symbols = vec!["AAPL".to_string(), "MSFT".to_string(), "GOOGL".to_string()];
    let batch_quotes = provider.fetch_quotes_batch(&symbols, start, end).await?;
    println!("Fetched {} total rows", batch_quotes.height());

    Ok(())
}
```

### Using the Cache

```rust
use perth_data::cache::SqliteCache;
use chrono::NaiveDate;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create cache
    let cache = SqliteCache::new("perth_cache.db")?;

    // Add symbols to universe
    cache.add_to_universe("AAPL", Some("Apple Inc."), Some("Technology"), None)?;
    cache.add_to_universe("MSFT", Some("Microsoft"), Some("Technology"), None)?;

    // Get universe
    let universe = cache.get_universe()?;
    println!("Universe: {:?}", universe);

    // Check if data is cached
    let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

    if !cache.has_quotes("AAPL", start, end)? {
        println!("Need to fetch data for AAPL");
        // Fetch and cache data...
    } else {
        let quotes = cache.get_quotes("AAPL", start, end)?;
        println!("Using cached data: {} rows", quotes.height());
    }

    // Get cache statistics
    let stats = cache.get_stats()?;
    println!("Cache stats: {:?}", stats);

    Ok(())
}
```

### Combining Fetching and Caching

```rust
use perth_data::{yahoo::YahooQuoteProvider, cache::SqliteCache};
use chrono::{Utc, Duration, NaiveDate};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = YahooQuoteProvider::new();
    let cache = SqliteCache::new("perth_cache.db")?;

    let symbol = "AAPL";
    let end = Utc::now();
    let start = end - Duration::days(365);

    let start_date = start.date_naive();
    let end_date = end.date_naive();

    // Check cache first
    let quotes = if cache.has_quotes(symbol, start_date, end_date)? {
        println!("Using cached data");
        cache.get_quotes(symbol, start_date, end_date)?
    } else {
        println!("Fetching from Yahoo Finance");
        let quotes = provider.fetch_quotes(symbol, start, end).await?;
        cache.put_quotes(&quotes)?;
        quotes
    };

    println!("Got {} rows for {}", quotes.height(), symbol);

    Ok(())
}
```

## Database Schema

### quotes

Stores OHLCV data for symbols.

```sql
CREATE TABLE quotes (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    adjusted_close REAL NOT NULL,
    cached_at TEXT NOT NULL,
    PRIMARY KEY (symbol, date)
);
```

### universe

Tracks the universe of symbols being analyzed.

```sql
CREATE TABLE universe (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT,
    added_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
```

### market_caps

Stores market capitalization data.

```sql
CREATE TABLE market_caps (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    market_cap REAL NOT NULL,
    cached_at TEXT NOT NULL,
    PRIMARY KEY (symbol, date)
);
```

### fundamentals

Stores fundamental data as JSON.

```sql
CREATE TABLE fundamentals (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    data TEXT NOT NULL,
    cached_at TEXT NOT NULL,
    PRIMARY KEY (symbol, date)
);
```

## Rate Limiting

The Yahoo Finance provider implements rate limiting to respect API constraints:

- Default: 1 request per second
- Customizable via `YahooQuoteProvider::with_rate_limit()`

```rust
use std::time::Duration;

let provider = YahooQuoteProvider::with_rate_limit(Duration::from_millis(2000));
```

## Error Handling

The crate provides a comprehensive `DataError` enum:

- `YahooApi`: Yahoo Finance API errors
- `Network`: Network/HTTP errors
- `Database`: SQLite errors
- `Parse`: Data parsing errors
- `InvalidDateRange`: Invalid date range
- `MissingData`: Missing data for symbol
- `Polars`: Polars DataFrame errors
- `RateLimit`: Rate limit exceeded
- `InvalidSymbol`: Invalid symbol
- `Cache`: Cache-related errors

## Testing

Run tests with:

```bash
cargo test
```

Note: Some tests require network access to Yahoo Finance API.

## Dependencies

- `polars`: DataFrame manipulation
- `yahoo_finance_api`: Yahoo Finance API client
- `rusqlite`: SQLite database
- `tokio`: Async runtime
- `chrono`: Date/time handling
- `thiserror`: Error handling
- `serde`/`serde_json`: Serialization

## License

MIT
