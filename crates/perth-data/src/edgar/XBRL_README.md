# XBRL Parsing for SEC EDGAR Filings

This module provides comprehensive XBRL (eXtensible Business Reporting Language) parsing capabilities for SEC EDGAR filings, focusing on 10-K and 10-Q reports.

## Overview

The implementation leverages the SEC's JSON API (https://data.sec.gov/api/xbrl/) which is significantly easier to parse and more reliable than raw XML XBRL files.

## Key Features

- **JSON API Integration**: Fetches and parses company facts from SEC's JSON API
- **Comprehensive Data Extraction**: Extracts all financial data points (facts) with full context
- **Rich Querying**: Multiple methods to query facts by concept, fiscal year, form type, etc.
- **Type Safety**: Strongly typed with proper error handling
- **US-GAAP Taxonomy**: Pre-defined constants for common financial concepts

## Core Types

### `XbrlFact`

Represents a single financial data point:

```rust
pub struct XbrlFact {
    pub concept: String,              // e.g., "us-gaap:NetIncomeLoss"
    pub value: f64,                   // The numeric value
    pub unit: String,                 // e.g., "USD", "shares"
    pub period_end: NaiveDate,        // End of reporting period
    pub period_start: Option<NaiveDate>, // Start (None for instant facts)
    pub form: Option<String>,         // e.g., "10-K", "10-Q"
    pub fiscal_year: Option<i32>,     // Fiscal year
    pub fiscal_period: Option<String>, // e.g., "FY", "Q1", "Q2", "Q3", "Q4"
}
```

Facts can be either:
- **Instant**: Point-in-time values (e.g., balance sheet items like Assets)
- **Duration**: Period-based values (e.g., income statement items like Net Income)

### `XbrlDocument`

A collection of XBRL facts from a company:

```rust
pub struct XbrlDocument {
    pub facts: Vec<XbrlFact>,
    pub entity_name: Option<String>,
    pub cik: Option<String>,
}
```

### `XbrlClient`

HTTP client for fetching data from SEC API:

```rust
pub struct XbrlClient {
    // ... internal fields
}
```

## Usage Examples

### Basic Usage

```rust
use perth_data::edgar::xbrl::{XbrlClient, concepts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let client = XbrlClient::new();

    // Fetch all facts for Apple Inc. (CIK: 0000320193)
    let doc = client.fetch_company_facts("0000320193").await?;

    // Get latest total assets
    if let Some(fact) = doc.get_latest_fact(concepts::balance_sheet::ASSETS) {
        println!("Total Assets: ${:.2}M", fact.value / 1_000_000.0);
    }

    Ok(())
}
```

### Query Methods

```rust
// Get specific fact by concept and date
let fact = doc.get_fact("us-gaap:Assets",
    NaiveDate::from_ymd_opt(2023, 12, 31).unwrap());

// Get latest fact for a concept
let latest = doc.get_latest_fact("us-gaap:NetIncomeLoss");

// Get all facts for a concept (sorted newest first)
let all_assets = doc.get_facts_by_concept("us-gaap:Assets");

// Get facts by fiscal year
let fy2023 = doc.get_facts_by_fiscal_year("us-gaap:Assets", 2023);

// Get facts by form type (e.g., only 10-K annual reports)
let annual_only = doc.get_facts_by_form("us-gaap:Assets", "10-K");

// Get all available concepts
let concepts = doc.get_concepts();
```

### Financial Analysis

```rust
// Calculate debt-to-equity ratio
if let (Some(liabilities), Some(equity)) = (
    doc.get_latest_fact(concepts::balance_sheet::LIABILITIES),
    doc.get_latest_fact(concepts::balance_sheet::STOCKHOLDERS_EQUITY),
) {
    let debt_to_equity = liabilities.value / equity.value;
    println!("Debt-to-Equity: {:.2}", debt_to_equity);
}

// Analyze revenue trends
let revenues = doc.get_facts_by_form(concepts::income_statement::REVENUES, "10-K");
for fact in revenues.iter().take(5) {
    println!("FY{}: ${:.2}B",
        fact.fiscal_year.unwrap(),
        fact.value / 1_000_000_000.0);
}
```

## US-GAAP Concepts

The module provides constants for common US-GAAP concepts:

### Balance Sheet
- `concepts::balance_sheet::ASSETS` - Total Assets
- `concepts::balance_sheet::LIABILITIES` - Total Liabilities
- `concepts::balance_sheet::STOCKHOLDERS_EQUITY` - Stockholders' Equity
- `concepts::balance_sheet::LONG_TERM_DEBT` - Long-term Debt (non-current)
- `concepts::balance_sheet::CASH` - Cash and Cash Equivalents
- `concepts::balance_sheet::CURRENT_ASSETS` - Current Assets
- `concepts::balance_sheet::CURRENT_LIABILITIES` - Current Liabilities

### Income Statement
- `concepts::income_statement::REVENUES` - Total Revenue
- `concepts::income_statement::NET_INCOME` - Net Income (Loss)
- `concepts::income_statement::COST_OF_REVENUE` - Cost of Revenue
- `concepts::income_statement::OPERATING_INCOME` - Operating Income (Loss)
- `concepts::income_statement::GROSS_PROFIT` - Gross Profit

### Cash Flow Statement
- `concepts::cash_flow::OPERATING_CASH_FLOW` - Operating Cash Flows
- `concepts::cash_flow::INVESTING_CASH_FLOW` - Investing Cash Flows
- `concepts::cash_flow::FINANCING_CASH_FLOW` - Financing Cash Flows

### Per-Share Metrics
- `concepts::per_share::EPS_BASIC` - Earnings Per Share - Basic
- `concepts::per_share::EPS_DILUTED` - Earnings Per Share - Diluted
- `concepts::per_share::SHARES_OUTSTANDING` - Common Stock Shares Outstanding

## SEC API Endpoints

### Company Facts
Fetches all available facts for a company:

```rust
let doc = client.fetch_company_facts("0000320193").await?;
```

URL: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json`

### Company Concept
Fetches data for a specific concept across all filings:

```rust
let doc = client.fetch_company_concept("0000320193", "us-gaap", "Assets").await?;
```

URL: `https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/Assets.json`

## Important Notes

### CIK Format
- CIKs are 10-digit numbers
- The client automatically pads CIKs to 10 digits
- You can pass either "320193" or "0000320193"

### User-Agent Requirement
The SEC requires a User-Agent header for all API requests. The default client uses:
```
perth-data/0.1.0 (https://github.com/factordynamics/perth)
```

You can customize it:
```rust
let client = XbrlClient::with_user_agent("MyCompany contact@example.com");
```

### Rate Limiting
Be respectful of SEC servers:
- Implement rate limiting in production
- Consider caching responses
- The SEC recommends no more than 10 requests per second

### Data Quality
- Not all companies report all concepts
- Concept names may vary (e.g., some use `Revenues`, others use `RevenueFromContractWithCustomerExcludingAssessedTax`)
- Always check for `None` when querying facts
- Validate fiscal periods match your expectations

## Example: Building a Factor Model

```rust
use perth_data::edgar::xbrl::{XbrlClient, concepts};

async fn calculate_book_to_market(cik: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let client = XbrlClient::new();
    let doc = client.fetch_company_facts(cik).await?;

    // Get latest book value (stockholders' equity)
    let equity = doc
        .get_latest_fact(concepts::balance_sheet::STOCKHOLDERS_EQUITY)
        .ok_or("No equity data")?;

    // Get shares outstanding
    let shares = doc
        .get_latest_fact(concepts::per_share::SHARES_OUTSTANDING)
        .ok_or("No shares data")?;

    // Get market cap from separate source (not in XBRL)
    let market_cap = get_market_cap(cik).await?;

    // Calculate book-to-market
    let book_value = equity.value;
    let book_to_market = book_value / market_cap;

    Ok(book_to_market)
}
```

## Testing

Run the tests:
```bash
cargo test -p perth-data --test xbrl_parsing_test
```

Run the example:
```bash
cargo run --example edgar_xbrl_example
```

## References

- [SEC EDGAR API Documentation](https://www.sec.gov/edgar/sec-api-documentation)
- [XBRL US GAAP Taxonomy](https://xbrl.us/xbrl-taxonomy/2023-us-gaap/)
- [SEC Company Search](https://www.sec.gov/cgi-bin/browse-edgar)

## Future Enhancements

Potential improvements:
1. Full XML XBRL parsing (currently returns error)
2. Caching layer for SEC API responses
3. Automatic retry with exponential backoff
4. Support for additional taxonomies (IFRS, country-specific)
5. Validation of fact consistency across filings
6. Automatic mapping of alternative concept names
