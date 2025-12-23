# perth

Perth: An institutional-grade equity factor model built on toraniko-rs.

## Overview

Perth is an umbrella crate that re-exports all Perth functionality and provides the CLI binary for factor analysis. It combines data fetching, factor computation, risk modeling, and reporting into a unified framework.

## Features

- **Comprehensive Factor Coverage**: Seven factor categories (Value, Momentum, Size, Volatility, Quality, Growth, Liquidity)
- **Advanced Risk Modeling**: Multi-factor risk decomposition with sophisticated covariance estimation
- **Universe Definitions**: Pre-configured universes (S&P 500) and sector classifications (GICS)
- **CLI Tool**: Command-line interface for factor analysis and reporting
- **Modular Architecture**: Clean separation of data, factors, risk, and output

## Crates

Perth re-exports functionality from four specialized crates:

- **`perth-data`**: Data fetching and caching (Yahoo Finance, SQLite)
- **`perth-factors`**: Factor implementations (all seven categories)
- **`perth-risk`**: Risk model (covariance estimation, specific risk)
- **`perth-output`**: Reporting and export (attribution, risk summary, CSV/JSON)

## Modules

- `universe`: Universe and sector definitions
  - `sp500`: S&P 500 constituent universe
  - `gics`: GICS sector classification

## Types

- **SP500Universe**: S&P 500 universe with constituents
- **GicsSector**: GICS sector enumeration
- **Universe**: Trait for defining investment universes

## Usage

### Using as Library

```rust
use perth::universe::sp500::SP500Universe;
use perth::factors::value::BookToPriceFactor;
use perth::risk::{RiskModel, EwmaCovarianceEstimator, SpecificRiskEstimator};
use toraniko_traits::Factor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define universe
    let universe = SP500Universe::new();
    println!("Analyzing {} securities", universe.symbols().len());

    // Create factors
    let value_factor = BookToPriceFactor::default();

    // Create risk model
    let cov_estimator = EwmaCovarianceEstimator::try_default()?;
    let spec_risk_estimator = SpecificRiskEstimator::default();
    let risk_model = RiskModel::default();

    // Use the framework...

    Ok(())
}
```

### Universe and Sectors

```rust
use perth::universe::{Universe, sp500::SP500Universe, gics::GicsSector};

let universe = SP500Universe::new();

// Get all symbols
let symbols = universe.symbols();

// Filter by sector
let tech_stocks = universe.filter_by_sector(GicsSector::InformationTechnology);

// Get sector for a symbol
if let Some(sector) = universe.get_sector("AAPL") {
    println!("AAPL sector: {:?}", sector);
}
```

### CLI Usage

Perth provides a command-line tool for factor analysis:

```bash
# Run factor analysis
perth analyze --universe sp500 --start 2024-01-01 --end 2024-12-31

# Generate risk report
perth risk --portfolio my_portfolio.csv --output risk_report.json

# Compute factor exposures
perth exposures --symbols AAPL,MSFT,GOOGL --factors value,momentum,size

# Export factor scores
perth scores --factor value --date 2024-12-31 --output value_scores.csv
```

Run `perth --help` for complete CLI documentation.

## Re-exports

All sub-crate functionality is re-exported:

```rust
use perth::data;        // perth-data
use perth::factors;     // perth-factors
use perth::risk;        // perth-risk
use perth::output;      // perth-output
```

## Dependencies

- `perth-data`: Data fetching and caching
- `perth-factors`: Factor implementations
- `perth-risk`: Risk modeling
- `perth-output`: Reporting and export
- `toraniko`: Core toraniko framework
- `toraniko-traits`: Trait definitions
- `toraniko-model`: Factor model implementation
- `toraniko-styles`: Style factor support
- `polars`: DataFrame operations
- `ndarray`: Array operations
- `tokio`: Async runtime
- `clap`: CLI framework (optional, with `cli` feature)

## Features

- `default`: Includes CLI
- `cli`: Enables command-line interface
- `full`: All features enabled

## License

MIT
