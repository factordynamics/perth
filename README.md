# perth

[![CI](https://github.com/factordynamics/perth/actions/workflows/ci.yml/badge.svg)](https://github.com/factordynamics/perth/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

Perth is an equity factor model for quantitative analysis. It fetches market data from Yahoo Finance and SEC EDGAR, computes factor exposures (beta, momentum, size, value, quality, growth, liquidity), and performs risk decomposition using covariance estimation techniques including EWMA, Ledoit-Wolf shrinkage, and Newey-West HAC. The underlying mathematical model is provided by [toraniko-rs](https://github.com/factordynamics/toraniko-rs).

The factor model decomposes asset returns as r = β × r_market + Σ(β_sector × r_sector) + Σ(β_style × r_style) + ε, separating total risk into systematic (factor) and idiosyncratic (specific) components.

## Quick Start

Analyze any stock using real market data:

```bash
just analyze UNH        # Default 5-year analysis
just analyze AAPL 2     # 2-year analysis
```

Example output:

```
Total Return: +52.73%  |  Factor-Explained: +42.90%  |  Idiosyncratic: +9.83%  |  R²: 81.4%

Factor                   Exposure   Contribution
Market                      1.000        +46.78%
Health_Care                 1.000        +14.11%
size                        0.973        +42.93%
amihud                     -0.954        -53.09%
medium_term_momentum       -0.807         -6.25%
```

## Development

Requires Rust 1.88+ and [just](https://github.com/casey/just). Run `just ci` to ensure all tests and lints pass.

## Attribution

Built on [toraniko-rs](https://github.com/factordynamics/toraniko-rs), a Rust port of the original [toraniko](https://github.com/0xfdf/toraniko) Python implementation.

## License

MIT License - see [LICENSE](LICENSE).
