# perth-factors

Factor implementations for the Perth institutional-grade factor model.

## Factors

This crate provides seven categories of equity factors:

- **Value**: Book-to-price, earnings yield
- **Momentum**: Short-term (1mo), medium-term (6mo), long-term (12mo) price momentum
- **Size**: Log market capitalization
- **Volatility**: Market beta, historical volatility, idiosyncratic volatility
- **Quality**: Return on equity (ROE), leverage
- **Growth**: Earnings growth, sales growth
- **Liquidity**: Turnover ratio, Amihud illiquidity measure

Each category includes individual factor implementations and composite factors that combine signals.

## Types

All factors implement the `Factor` and `StyleFactor` traits from `toraniko-traits`:

- **Value Factors**: `BookToPriceFactor`, `EarningsYieldFactor`, `CompositeValueFactor`
- **Momentum Factors**: `ShortTermMomentum`, `MediumTermMomentum`, `LongTermMomentum`, `CompositeMomentumFactor`
- **Size Factors**: `LogMarketCapFactor`
- **Volatility Factors**: `BetaFactor`, `HistoricalVolatilityFactor`, `IdiosyncraticVolatilityFactor`, `CompositeVolatilityFactor`
- **Quality Factors**: `RoeFactor`, `LeverageFactor`, `CompositeQualityFactor`
- **Growth Factors**: `EarningsGrowthFactor`, `SalesGrowthFactor`, `CompositeGrowthFactor`
- **Liquidity Factors**: `TurnoverFactor`, `AmihudFactor`, `CompositeLiquidityFactor`

## Usage

```rust
use perth_factors::value::BookToPriceFactor;
use perth_factors::momentum::MediumTermMomentum;
use toraniko_traits::Factor;
use polars::prelude::*;

// Create a factor instance
let value_factor = BookToPriceFactor::default();
let momentum_factor = MediumTermMomentum::default();

// Compute factor scores from market data
// (Factor trait provides compute() method)
```

### Factor Registry

The `registry` module provides introspection of available factors:

```rust
use perth_factors::{available_factors, factors_by_category, get_factor_info};

// List all available factors
let all_factors = available_factors();

// Get factors by category
let value_factors = factors_by_category(FactorCategory::Value);

// Get detailed information about a specific factor
let info = get_factor_info("BookToPrice").unwrap();
println!("{}: {}", info.name, info.description);
```

## Dependencies

- `toraniko-traits`: Factor trait definitions
- `toraniko-math`: Mathematical utilities for factor calculations
- `polars`: DataFrame operations
- `chrono`: Date/time handling
- `serde`: Serialization support

## License

MIT
