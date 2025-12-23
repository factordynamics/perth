# perth

An institutional-grade factor model for quantitative equity analysis, built on [toraniko-rs](https://github.com/factordynamics/toraniko-rs).

[![CI](https://github.com/factordynamics/perth/actions/workflows/ci.yml/badge.svg)](https://github.com/factordynamics/perth/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

**Perth** extends toraniko-rs with comprehensive data fetching, risk modeling, and factor analytics for building production-ready equity factor models. It provides:

- **Data Integration**: Yahoo Finance quotes and SEC EDGAR fundamentals
- **Risk Modeling**: Covariance estimation (EWMA, Ledoit-Wolf, Newey-West)
- **Factor Analysis**: Complete factor attribution and decomposition
- **Output Reporting**: CSV, JSON, and formatted reports

### Mathematical Model

The factor model decomposes asset returns as:

```
r_asset = β_market * r_market + Σ(β_sector * r_sector) + Σ(β_style * r_style) + ε
```

Risk decomposition separates systematic and idiosyncratic components:

```
σ²_total = σ²_factor + σ²_specific
σ²_factor = β' * Σ_factor * β
```

## Features

- **Data Providers**: Yahoo Finance, SEC EDGAR XBRL fundamentals
- **Covariance Estimation**: EWMA, Ledoit-Wolf shrinkage, Newey-West HAC
- **Volatility Regimes**: Automatic regime detection and risk scaling
- **Factor Exposures**: Beta, momentum, size, value, quality, growth, liquidity
- **Specific Risk**: Bayesian shrinkage for stable idiosyncratic risk estimates
- **Caching**: SQLite-based data cache for efficient backtesting

## Crate Structure

| Crate | Description |
|-------|-------------|
| `perth` | Umbrella crate with CLI and universe management |
| `perth-bin` | Command-line interface binary |
| `perth-data` | Data fetching (Yahoo, EDGAR) and caching |
| `perth-factors` | Factor implementations and registry |
| `perth-risk` | Risk model (covariance, specific risk, regimes) |
| `perth-output` | Reporting and export (CSV, JSON, markdown) |

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
perth = "0.1"
```

Or install the CLI:

```bash
cargo install perth-bin
```

## Quick Start

### Factor Attribution Analysis

Analyze any stock using real market data:

```bash
just analyze UNH        # Default 5-year analysis
just analyze AAPL 2     # 2-year analysis
```

Example output for UNH (5-year analysis, 119 stocks, 11 GICS sectors, 5 style factors):

```
Total Return: +52.73%  |  Factor-Explained: +42.90%  |  Idiosyncratic: +9.83%  |  R²: 81.4%

Factor                   Exposure   Contribution
Market                      1.000        +46.78%
Health_Care                 1.000        +14.11%
size                        0.973        +42.93%
amihud                     -0.954        -53.09%
medium_term_momentum       -0.807         -6.25%
```

### Risk Analysis

```bash
just risk UNH           # Full risk analysis
just universe           # Show S&P 500 universe
just sectors            # List GICS sectors
```

### Programmatic Usage

```rust
use perth::prelude::*;
use perth_data::yahoo::quotes::YahooQuoteProvider;
use perth_risk::covariance::{LedoitWolfEstimator, CovarianceEstimator};

// Fetch market data
let provider = YahooQuoteProvider::new();
let quotes = provider.fetch_quotes("AAPL", start, end).await?;

// Estimate factor covariance with shrinkage
let estimator = LedoitWolfEstimator::default();
let covariance = estimator.estimate(&factor_returns)?;

// Analyze volatility regime
let detector = VolatilityRegimeDetector::default();
let (regime, scale) = detector.analyze(&market_returns);
```

## Development

### Prerequisites

- Rust 1.88+ (2024 edition)
- [just](https://github.com/casey/just) task runner

### Commands

```bash
# Run all CI checks
just ci

# Run tests
just test

# Format and lint
just fix

# Build release
just build

# Analyze a stock
just analyze AAPL

# Generate documentation
just doc
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [toraniko-rs](https://github.com/factordynamics/toraniko-rs) - Core factor model estimation
- [toraniko](https://github.com/0xfdf/toraniko) - Original Python implementation
