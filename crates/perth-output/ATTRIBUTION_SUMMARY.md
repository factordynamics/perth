# Factor Attribution & Risk Summary

This document describes the factor attribution and risk summary functionality added to the `perth-output` crate.

## Overview

The enhancement adds comprehensive factor attribution and risk decomposition capabilities, enabling users to:

- Decompose security and portfolio returns into factor contributions
- Calculate specific (idiosyncratic) returns
- Analyze risk contributions from individual factors
- Compute Value at Risk (VaR) metrics
- Generate formatted reports in ASCII table and Markdown formats

## New Modules

### `attribution.rs` (876 lines)

Provides factor attribution analysis for decomposing returns.

#### Key Structures

**`FactorAttribution`**
- Represents a single factor's contribution to returns
- Fields:
  - `factor_name`: Factor identifier (e.g., "Market", "Size")
  - `exposure`: Security's loading/beta for the factor
  - `factor_return`: Factor's return during the period
  - `contribution`: Exposure × factor_return
  - `contribution_pct`: Contribution as percentage of total return

**`SecurityAttribution`**
- Decomposes a single security's return into factor and specific components
- Fields:
  - `symbol`: Security identifier
  - `period_start`, `period_end`: Analysis period
  - `total_return`: Total security return
  - `factor_return`: Sum of all factor contributions
  - `specific_return`: Residual (idiosyncratic) return
  - `factors`: Vector of `FactorAttribution` instances
- Methods:
  - `r_squared()`: Calculate R² (proportion of variance explained)
  - `to_ascii_table()`: Generate formatted terminal output
  - `to_markdown()`: Generate Markdown documentation

**`PortfolioAttribution`**
- Aggregates attribution across multiple securities
- Supports equal-weighted or custom-weighted portfolios
- Fields:
  - `portfolio_name`: Portfolio identifier
  - `total_return`: Weighted portfolio return
  - `factor_return`: Weighted factor contributions
  - `specific_return`: Portfolio-level specific return
  - `factors`: Aggregated factor attributions
  - `securities`: Individual security attributions
- Methods:
  - `new()`: Create equal-weighted portfolio
  - `new_weighted()`: Create custom-weighted portfolio
  - `to_ascii_table()`, `to_markdown()`: Formatting methods

### `summary.rs` (690 lines)

Provides risk analysis and decomposition capabilities.

#### Key Structures

**`FactorRiskContribution`**
- Represents a factor's contribution to portfolio risk
- Fields:
  - `factor_name`: Factor identifier
  - `exposure`: Portfolio exposure to factor
  - `factor_volatility`: Factor's standard deviation
  - `marginal_contribution`: Marginal contribution to risk (MCR)
  - `risk_contribution`: Contribution to total variance
  - `risk_contribution_pct`: Percentage of total risk

**`RiskSummary`**
- Comprehensive risk breakdown for portfolios or securities
- Fields:
  - `name`: Entity name
  - `period_start`, `period_end`: Analysis period
  - `total_risk`: Total portfolio volatility (σ)
  - `factor_risk`: Systematic risk from factors
  - `specific_risk`: Idiosyncratic risk
  - `var_95`, `var_99`: Value at Risk at 95% and 99% confidence
  - `factor_contributions`: Individual factor risk contributions
  - `portfolio_value`: Optional value for monetary VaR
- Methods:
  - `set_portfolio_value()`: Set value for monetary VaR calculation
  - `var_95_monetary()`, `var_99_monetary()`: Get VaR in currency units
  - `factor_risk_ratio()`: Proportion of risk from factors
  - `specific_risk_ratio()`: Proportion of specific risk
  - `to_ascii_table()`, `to_markdown()`: Formatting methods

#### Functions

**`generate_risk_summary()`**
- Creates a risk summary from factor exposures and volatilities
- Parameters:
  - `name`: Portfolio/security name
  - `period_start`, `period_end`: Analysis period
  - `exposures`: HashMap of factor names to exposures
  - `factor_volatilities`: HashMap of factor names to volatilities
  - `specific_volatility`: Idiosyncratic volatility
- Returns: Complete `RiskSummary` with calculated metrics
- Note: Uses diagonal covariance assumption (factors uncorrelated)

## Usage Examples

### Security Attribution

```rust
use perth_output::{FactorAttribution, SecurityAttribution};
use chrono::NaiveDate;

let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

// Define factor contributions
let factors = vec![
    FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15),
    FactorAttribution::new("Size".to_string(), 0.5, 0.05, 0.15),
];

// Create attribution
let attribution = SecurityAttribution::new(
    "AAPL".to_string(),
    start,
    end,
    0.15,  // 15% total return
    factors,
);

// Display results
println!("{}", attribution.to_ascii_table());
println!("R-squared: {:.4}", attribution.r_squared());
```

### Portfolio Attribution

```rust
use perth_output::PortfolioAttribution;

// Create weighted portfolio from security attributions
let portfolio = PortfolioAttribution::new_weighted(
    "Tech Portfolio".to_string(),
    vec![aapl_attribution, msft_attribution],
    vec![0.6, 0.4],  // 60% AAPL, 40% MSFT
);

// Export as Markdown
println!("{}", portfolio.to_markdown());
```

### Risk Summary

```rust
use perth_output::generate_risk_summary;
use std::collections::HashMap;

let mut exposures = HashMap::new();
exposures.insert("Market".to_string(), 1.2);
exposures.insert("Size".to_string(), 0.5);

let mut volatilities = HashMap::new();
volatilities.insert("Market".to_string(), 0.15);
volatilities.insert("Size".to_string(), 0.10);

let mut summary = generate_risk_summary(
    "Portfolio".to_string(),
    start,
    end,
    exposures,
    volatilities,
    0.05,  // 5% specific volatility
);

// Add portfolio value for monetary VaR
summary.set_portfolio_value(1_000_000.0);

println!("{}", summary.to_ascii_table());
```

## Output Formats

### ASCII Table Format

The `to_ascii_table()` method generates formatted output suitable for terminal display:

```
Factor Attribution: AAPL
Period: 2024-01-01 to 2024-12-31
================================================================================
Factor               Exposure       Return Contribution   % of Total
--------------------------------------------------------------------------------
Market                 1.2000       10.00%       12.00%       80.00%
Size                   0.5000        5.00%        2.50%       16.67%
--------------------------------------------------------------------------------
Factor Return                                    14.50%
Specific Return                                   0.50%
Total Return                                     15.00%
================================================================================
R-squared: 0.9406
```

### Markdown Format

The `to_markdown()` method generates documentation-ready output:

```markdown
# Factor Attribution: AAPL

**Period:** 2024-01-01 to 2024-12-31

| Factor | Exposure | Return | Contribution | % of Total |
|--------|----------|--------|--------------|------------|
| Market | 1.2000 | 10.00% | 12.00% | 80.00% |
| Size | 0.5000 | 5.00% | 2.50% | 16.67% |

## Summary

- **Factor Return:** 14.50%
- **Specific Return:** 0.50%
- **Total Return:** 15.00%
- **R-squared:** 0.9406
```

## Testing

The implementation includes comprehensive unit tests:

### Attribution Module Tests (9 tests)
- `test_factor_attribution_creation`: Basic FactorAttribution construction
- `test_security_attribution`: SecurityAttribution with multiple factors
- `test_security_attribution_ascii_table`: ASCII table formatting
- `test_security_attribution_markdown`: Markdown formatting
- `test_portfolio_attribution_equal_weight`: Equal-weighted portfolios
- `test_portfolio_attribution_weighted`: Custom-weighted portfolios
- `test_portfolio_attribution_invalid_weights`: Weight validation
- `test_portfolio_ascii_table`: Portfolio ASCII output
- `test_portfolio_markdown`: Portfolio Markdown output

### Summary Module Tests (9 tests)
- `test_factor_risk_contribution`: Basic FactorRiskContribution
- `test_risk_summary_creation`: RiskSummary construction
- `test_risk_summary_with_portfolio_value`: Monetary VaR calculation
- `test_risk_ratios`: Risk ratio calculations
- `test_generate_risk_summary`: Risk summary generation
- `test_risk_summary_ascii_table`: ASCII table formatting
- `test_risk_summary_markdown`: Markdown formatting
- `test_factor_risk_contribution_display`: Display trait
- `test_risk_summary_display`: Display trait

All tests pass successfully (34/35 total crate tests pass; 1 pre-existing failure in export module).

## Example Program

A complete demonstration is available in `examples/attribution_demo.rs`:

```bash
cargo run --package perth-output --example attribution_demo
```

This example shows:
1. Creating factor attributions for multiple securities
2. Building a weighted portfolio
3. Generating risk summaries with VaR calculations
4. Exporting results in multiple formats

## Technical Notes

### Risk Calculations

The risk summary uses the following formulas:

1. **Factor Risk**: `sqrt(Σ β_i² σ_i²)` (diagonal covariance assumption)
2. **Total Risk**: `sqrt(factor_variance + specific_variance)`
3. **95% VaR**: `1.645 × σ` (normal distribution assumption)
4. **99% VaR**: `2.326 × σ` (normal distribution assumption)

### Design Decisions

1. **Serde Support**: All structures implement `Serialize` and `Deserialize` for easy JSON/binary export
2. **Display Traits**: All structures implement `Display` for quick debugging
3. **Builder Pattern**: Risk summary uses a functional generation pattern
4. **Formatting**: Dual output formats (ASCII + Markdown) for different use cases
5. **Validation**: Portfolio weights must sum to 1.0 (enforced with assertions)

## Future Enhancements

Potential improvements for future versions:

1. **Full Covariance Matrix**: Support non-diagonal factor covariances
2. **Historical VaR**: Monte Carlo and historical simulation methods
3. **Attribution Over Time**: Time-series attribution analysis
4. **Factor Interactions**: Cross-factor contribution analysis
5. **PDF Export**: Direct PDF report generation
6. **Interactive Visualization**: HTML/JavaScript chart generation

## Dependencies

No new dependencies were added. The implementation uses only existing crate dependencies:
- `chrono`: Date handling
- `serde`: Serialization
- Standard library collections (HashMap)

## API Stability

All new public APIs are documented and tested. The API is considered stable for v0.1.0 release.
