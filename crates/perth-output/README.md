# perth-output

Output and reporting for the Perth factor model.

## Features

- **Factor Attribution**: Decompose returns into factor contributions
- **Risk Summary**: Risk decomposition showing factor risk contributions
- **Export**: Export data to CSV and JSON formats
- **Report Generation**: Structured report creation with builder pattern

## Modules

- `attribution`: Factor attribution analysis
  - Security-level attribution
  - Portfolio-level attribution
  - Factor contribution breakdown
- `summary`: Risk summary generation
  - Factor risk contributions
  - Total risk calculation
  - Risk decomposition tables
- `export`: Data export functionality
  - CSV export
  - JSON export
  - Portfolio holdings
  - Factor exposures
  - Risk decomposition
- `report`: Report generation and serialization

## Types

- **FactorAttribution**: Single factor's contribution to returns
- **SecurityAttribution**: Attribution for a single security
- **PortfolioAttribution**: Attribution for an entire portfolio
- **RiskSummary**: Risk decomposition summary
- **FactorRiskContribution**: Individual factor's risk contribution
- **Report**: Structured report container
- **Exporter**: Export utility for various data formats
- **ExportFormat**: CSV or JSON export formats

## Usage

### Factor Attribution

```rust,ignore
use perth_output::{FactorAttribution, SecurityAttribution};
use chrono::NaiveDate;

let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

let factors = vec![
    FactorAttribution::new("Market".to_string(), 1.2, 0.10, 0.12),
    FactorAttribution::new("Size".to_string(), 0.5, 0.05, 0.025),
    FactorAttribution::new("Value".to_string(), -0.3, -0.02, -0.006),
];

let attribution = SecurityAttribution::new(
    "AAPL".to_string(),
    start,
    end,
    0.15,  // total return
    factors,
);

// Display as ASCII table
println!("{}", attribution.to_ascii_table());

// Export to JSON
let json = serde_json::to_string_pretty(&attribution)?;
```

### Risk Summary

```rust,ignore
use perth_output::generate_risk_summary;
use chrono::NaiveDate;
use std::collections::HashMap;

let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

let mut exposures = HashMap::new();
exposures.insert("Market".to_string(), 1.2);
exposures.insert("Size".to_string(), 0.5);

let mut volatilities = HashMap::new();
volatilities.insert("Market".to_string(), 0.15);
volatilities.insert("Size".to_string(), 0.08);

let summary = generate_risk_summary(
    "Portfolio".to_string(),
    start,
    end,
    exposures,
    volatilities,
    0.05,  // specific risk
);

// Display as ASCII table
println!("{}", summary.to_ascii_table());
```

### Export to CSV/JSON

```rust,ignore
use perth_output::{Exporter, ExportFormat, PortfolioHolding, PortfolioExport};
use chrono::NaiveDate;

let date = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();

let holdings = vec![
    PortfolioHolding {
        symbol: "AAPL".to_string(),
        weight: 0.05,
        shares: Some(100.0),
        price: Some(150.0),
    },
    PortfolioHolding {
        symbol: "MSFT".to_string(),
        weight: 0.04,
        shares: Some(50.0),
        price: Some(380.0),
    },
];

let portfolio = PortfolioExport {
    date,
    holdings,
    total_value: Some(30000.0),
};

// Export to CSV
Exporter::export_portfolio(&portfolio, "portfolio.csv", ExportFormat::Csv)?;

// Export to JSON
Exporter::export_portfolio(&portfolio, "portfolio.json", ExportFormat::Json)?;
```

### Report Builder

```rust,ignore
use perth_output::{ReportBuilder, Report};
use std::collections::HashMap;

let mut metadata = HashMap::new();
metadata.insert("author".to_string(), "Perth System".to_string());
metadata.insert("date".to_string(), "2024-12-31".to_string());

let report = ReportBuilder::new()
    .title("Monthly Factor Analysis")
    .description("Factor performance and risk analysis for December 2024")
    .metadata(metadata)
    .add_section("Attribution", attribution_data)
    .add_section("Risk", risk_data)
    .build()?;

// Serialize report
let json = report.to_json()?;
```

## Dependencies

- `polars`: DataFrame operations
- `serde`/`serde_json`: JSON serialization
- `csv`: CSV export
- `chrono`: Date/time handling
- `thiserror`: Error handling

## License

MIT
