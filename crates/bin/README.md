# perth-bin

Command-line interface for the Perth factor model.

## Installation

```bash
cargo install perth-bin
```

## Usage

```bash
# Analyze factor attribution for a stock
perth analyze UNH
perth analyze AAPL --years 3

# Run risk analysis
perth risk --symbol UNH
perth risk --covariance --format json

# Show universe information
perth universe
perth universe --sector healthcare
perth universe --list-sectors

# Update data cache
perth update --quotes
perth update --fundamentals
perth update --full
```

## Commands

### `analyze`

Analyze factor attribution for an individual stock. Fetches real market data from Yahoo Finance and computes:
- Performance metrics (total return, volatility, Sharpe ratio, max drawdown)
- Factor exposures (market beta, momentum, size, volatility)
- Return attribution (factor-explained vs idiosyncratic)

### `risk`

Run risk analysis including:
- Factor covariance estimation (EWMA with Ledoit-Wolf shrinkage)
- Volatility regime detection
- Specific risk estimation

### `universe`

Display S&P 500 universe information including:
- Total constituents
- Breakdown by GICS sector
- Filter by specific sector

### `update`

Update the local data cache with fresh market data.
