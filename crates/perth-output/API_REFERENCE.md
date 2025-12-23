# Perth Output - Factor Attribution API Reference

## Module: `attribution`

### `FactorAttribution`

Represents a single factor's contribution to a security's return.

```rust
pub struct FactorAttribution {
    pub factor_name: String,
    pub exposure: f64,
    pub factor_return: f64,
    pub contribution: f64,
    pub contribution_pct: f64,
}
```

#### Constructor

```rust
pub fn new(
    factor_name: String,
    exposure: f64,
    factor_return: f64,
    total_return: f64
) -> Self
```

**Parameters:**
- `factor_name`: Name of the factor (e.g., "Market", "Size", "Value")
- `exposure`: Security's exposure/loading to the factor
- `factor_return`: Factor's return during the period
- `total_return`: Total security return (for percentage calculation)

**Returns:** `FactorAttribution` with calculated contribution and percentage

**Example:**
```rust
let attribution = FactorAttribution::new(
    "Market".to_string(),
    1.2,      // 1.2x market exposure
    0.10,     // 10% market return
    0.15,     // 15% total return
);
assert_eq!(attribution.contribution, 0.12); // 1.2 * 0.10
```

---

### `SecurityAttribution`

Decomposes a security's return into factor and specific components.

```rust
pub struct SecurityAttribution {
    pub symbol: String,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub total_return: f64,
    pub factor_return: f64,
    pub specific_return: f64,
    pub factors: Vec<FactorAttribution>,
}
```

#### Constructor

```rust
pub fn new(
    symbol: String,
    period_start: NaiveDate,
    period_end: NaiveDate,
    total_return: f64,
    factors: Vec<FactorAttribution>,
) -> Self
```

**Parameters:**
- `symbol`: Security identifier (e.g., "AAPL")
- `period_start`: Start date of analysis period
- `period_end`: End date of analysis period
- `total_return`: Total security return over the period
- `factors`: Vector of factor attributions

**Returns:** `SecurityAttribution` with calculated factor_return and specific_return

#### Methods

##### `r_squared() -> f64`

Calculate the R-squared (proportion of variance explained by factors).

**Returns:** Value between 0.0 and 1.0

##### `to_ascii_table() -> String`

Generate formatted ASCII table for terminal display.

**Returns:** Multi-line string with formatted table

##### `to_markdown() -> String`

Generate Markdown-formatted documentation.

**Returns:** Markdown string with table and summary

**Example:**
```rust
let factors = vec![
    FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.15),
    FactorAttribution::new("Size".to_string(), 0.5, 0.05, 0.15),
];

let attribution = SecurityAttribution::new(
    "AAPL".to_string(),
    start,
    end,
    0.15,
    factors,
);

println!("{}", attribution.to_ascii_table());
println!("R²: {:.4}", attribution.r_squared());
```

---

### `PortfolioAttribution`

Aggregates attribution across multiple securities.

```rust
pub struct PortfolioAttribution {
    pub portfolio_name: String,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub total_return: f64,
    pub factor_return: f64,
    pub specific_return: f64,
    pub factors: Vec<FactorAttribution>,
    pub securities: Vec<SecurityAttribution>,
}
```

#### Constructors

##### `new(portfolio_name: String, securities: Vec<SecurityAttribution>) -> Self`

Create equal-weighted portfolio attribution.

**Parameters:**
- `portfolio_name`: Portfolio identifier
- `securities`: Vector of security attributions

**Returns:** `PortfolioAttribution` with equal weights (1/N)

##### `new_weighted(portfolio_name: String, securities: Vec<SecurityAttribution>, weights: Vec<f64>) -> Self`

Create custom-weighted portfolio attribution.

**Parameters:**
- `portfolio_name`: Portfolio identifier
- `securities`: Vector of security attributions
- `weights`: Vector of weights (must sum to 1.0)

**Returns:** `PortfolioAttribution` with specified weights

**Panics:** If weights and securities have different lengths or weights don't sum to ~1.0

#### Methods

##### `r_squared() -> f64`

Portfolio-level R-squared.

##### `to_ascii_table() -> String`

Generate formatted ASCII table including individual securities.

##### `to_markdown() -> String`

Generate Markdown documentation.

**Example:**
```rust
let portfolio = PortfolioAttribution::new_weighted(
    "Tech Portfolio".to_string(),
    vec![aapl_attribution, msft_attribution],
    vec![0.6, 0.4],  // 60% AAPL, 40% MSFT
);

println!("{}", portfolio.to_markdown());
```

---

## Module: `summary`

### `FactorRiskContribution`

Represents a factor's contribution to portfolio risk.

```rust
pub struct FactorRiskContribution {
    pub factor_name: String,
    pub exposure: f64,
    pub factor_volatility: f64,
    pub marginal_contribution: f64,
    pub risk_contribution: f64,
    pub risk_contribution_pct: f64,
}
```

#### Constructor

```rust
pub fn new(
    factor_name: String,
    exposure: f64,
    factor_volatility: f64,
    marginal_contribution: f64,
    total_risk: f64,
) -> Self
```

**Parameters:**
- `factor_name`: Factor identifier
- `exposure`: Portfolio exposure to the factor
- `factor_volatility`: Factor's standard deviation
- `marginal_contribution`: Marginal contribution to risk (MCR)
- `total_risk`: Total portfolio risk (for percentage calculation)

**Returns:** `FactorRiskContribution` with calculated values

---

### `RiskSummary`

Comprehensive risk analysis for a portfolio or security.

```rust
pub struct RiskSummary {
    pub name: String,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub total_risk: f64,
    pub factor_risk: f64,
    pub specific_risk: f64,
    pub var_95: f64,
    pub var_99: f64,
    pub factor_contributions: Vec<FactorRiskContribution>,
    pub portfolio_value: Option<f64>,
}
```

#### Constructor

```rust
pub fn new(
    name: String,
    period_start: NaiveDate,
    period_end: NaiveDate,
    total_risk: f64,
    factor_risk: f64,
    specific_risk: f64,
    factor_contributions: Vec<FactorRiskContribution>,
) -> Self
```

**Parameters:**
- `name`: Portfolio or security name
- `period_start`: Start date of analysis
- `period_end`: End date of analysis
- `total_risk`: Total risk (standard deviation)
- `factor_risk`: Risk from factors (systematic)
- `specific_risk`: Idiosyncratic risk
- `factor_contributions`: Individual factor risk contributions

**Returns:** `RiskSummary` with calculated VaR metrics

#### Methods

##### `set_portfolio_value(&mut self, value: f64)`

Set portfolio value for monetary VaR calculations.

##### `var_95_monetary() -> Option<f64>`

Get 95% VaR in monetary terms.

**Returns:** `Some(value)` if portfolio_value is set, `None` otherwise

##### `var_99_monetary() -> Option<f64>`

Get 99% VaR in monetary terms.

##### `factor_risk_ratio() -> f64`

Proportion of risk explained by factors.

**Returns:** factor_risk / total_risk

##### `specific_risk_ratio() -> f64`

Proportion of risk from specific sources.

**Returns:** specific_risk / total_risk

##### `to_ascii_table() -> String`

Generate formatted ASCII table.

##### `to_markdown() -> String`

Generate Markdown documentation.

**Example:**
```rust
let mut summary = RiskSummary::new(
    "Portfolio".to_string(),
    start,
    end,
    0.20,
    0.18,
    0.05,
    factors,
);

summary.set_portfolio_value(1_000_000.0);
println!("95% VaR: ${:.2}", summary.var_95_monetary().unwrap());
```

---

### `generate_risk_summary` (Function)

Generate a risk summary from factor exposures and volatilities.

```rust
pub fn generate_risk_summary(
    name: String,
    period_start: NaiveDate,
    period_end: NaiveDate,
    exposures: HashMap<String, f64>,
    factor_volatilities: HashMap<String, f64>,
    specific_volatility: f64,
) -> RiskSummary
```

**Parameters:**
- `name`: Portfolio/security name
- `period_start`: Start date
- `period_end`: End date
- `exposures`: HashMap mapping factor names to exposures
- `factor_volatilities`: HashMap mapping factor names to volatilities (σ)
- `specific_volatility`: Idiosyncratic volatility

**Returns:** Complete `RiskSummary` with all metrics calculated

**Note:** Uses diagonal covariance assumption (factors are uncorrelated)

**Example:**
```rust
use std::collections::HashMap;

let mut exposures = HashMap::new();
exposures.insert("Market".to_string(), 1.2);
exposures.insert("Size".to_string(), 0.5);

let mut volatilities = HashMap::new();
volatilities.insert("Market".to_string(), 0.15);
volatilities.insert("Size".to_string(), 0.10);

let summary = generate_risk_summary(
    "Portfolio".to_string(),
    start,
    end,
    exposures,
    volatilities,
    0.05,
);

println!("{}", summary.to_ascii_table());
```

---

## Traits Implemented

All structures implement:
- `Debug`: For debugging output
- `Clone`: For duplication
- `Serialize`, `Deserialize` (serde): For JSON/binary serialization
- `PartialEq`: For equality comparison
- `Display` (std::fmt): For formatted string output

## Output Format Examples

### ASCII Table (SecurityAttribution)

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

### Markdown (PortfolioAttribution)

```markdown
# Portfolio Factor Attribution: Tech Portfolio

**Period:** 2024-01-01 to 2024-12-31
**Number of Securities:** 2

## Portfolio-Level Attribution

| Factor | Exposure | Return | Contribution | % of Total |
|--------|----------|--------|--------------|------------|
| Market | 1.1000 | 10.00% | 11.00% | 78.57% |
| Size | 0.5500 | 5.00% | 2.75% | 19.64% |

### Summary

- **Factor Return:** 13.75%
- **Specific Return:** 0.25%
- **Total Return:** 14.00%
- **Portfolio R-squared:** 0.9821

## Individual Securities

| Symbol | Total Return | Factor Return | Specific Return | R-squared |
|--------|--------------|---------------|-----------------|-----------|
| AAPL | 15.00% | 14.50% | 0.50% | 0.9406 |
| MSFT | 13.00% | 13.00% | 0.00% | 1.0000 |
```

---

## Error Handling

### Panics

- `PortfolioAttribution::new_weighted`: Panics if:
  - `securities.len() != weights.len()`
  - `weights.sum() != 1.0` (within 1e-6 tolerance)

### Edge Cases

All functions handle edge cases gracefully:
- Zero returns (no division by zero)
- Negative returns (correctly formatted)
- Empty factor lists (returns zero attribution)
- Empty security lists (returns empty portfolio)

---

## Performance Considerations

- Factor aggregation: O(n × m) where n = securities, m = factors per security
- Sorting: Factors sorted by contribution (O(k log k) where k = unique factors)
- String formatting: Linear in output size
- Memory: Structures use `Vec` for dynamic sizing

---

## Version

API version: 0.1.0
Module: perth-output
