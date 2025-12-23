//! Example demonstrating XBRL parsing from SEC EDGAR filings.
//!
//! This example shows how to:
//! 1. Fetch company facts from the SEC API
//! 2. Extract specific financial metrics
//! 3. Query facts by fiscal year and form type
//!
//! Run with:
//! ```bash
//! cargo run --example edgar_xbrl_example
//! ```

use perth_data::edgar::xbrl::{XbrlClient, concepts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== SEC EDGAR XBRL Example ===\n");

    // Create an XBRL client
    let client = XbrlClient::new();

    // Fetch all company facts for Apple Inc. (CIK: 0000320193)
    println!("Fetching company facts for Apple Inc...");
    let doc = client.fetch_company_facts("0000320193").await?;

    println!("Company: {}", doc.entity_name.as_ref().unwrap());
    println!("CIK: {}", doc.cik.as_ref().unwrap());
    println!("Total facts loaded: {}\n", doc.facts.len());

    // Example 1: Get latest Total Assets
    println!("=== Example 1: Latest Total Assets ===");
    if let Some(fact) = doc.get_latest_fact(concepts::balance_sheet::ASSETS) {
        println!(
            "Total Assets: ${:.2}M (as of {})",
            fact.value / 1_000_000.0,
            fact.period_end
        );
        println!("Form: {:?}", fact.form);
        println!("Fiscal Year: {:?}", fact.fiscal_year);
        println!("Fiscal Period: {:?}\n", fact.fiscal_period);
    }

    // Example 2: Get latest Net Income
    println!("=== Example 2: Latest Net Income ===");
    if let Some(fact) = doc.get_latest_fact(concepts::income_statement::NET_INCOME) {
        println!("Net Income: ${:.2}M", fact.value / 1_000_000.0);
        if let Some(start) = fact.period_start {
            println!("Period: {} to {}", start, fact.period_end);
            println!("Duration: {} days", fact.duration_days().unwrap());
        }
        println!();
    }

    // Example 3: Get historical Assets (last 5 annual reports)
    println!("=== Example 3: Historical Assets (10-K only) ===");
    let asset_facts = doc.get_facts_by_form(concepts::balance_sheet::ASSETS, "10-K");
    for (i, fact) in asset_facts.iter().take(5).enumerate() {
        println!(
            "{}. FY{}: ${:.2}B ({})",
            i + 1,
            fact.fiscal_year.unwrap_or(0),
            fact.value / 1_000_000_000.0,
            fact.period_end
        );
    }
    println!();

    // Example 4: Get EPS for a specific fiscal year
    println!("=== Example 4: EPS for Fiscal Year 2023 ===");
    let eps_facts = doc.get_facts_by_fiscal_year(concepts::per_share::EPS_BASIC, 2023);
    for fact in eps_facts {
        println!(
            "EPS (Basic): ${:.2} - {} ({:?})",
            fact.value, fact.period_end, fact.form
        );
    }
    println!();

    // Example 5: Get Revenue trends
    println!("=== Example 5: Revenue Trends (Last 3 Years) ===");
    let revenue_facts = doc.get_facts_by_concept(concepts::income_statement::REVENUES);

    // Filter to get only annual (10-K) reports
    let annual_revenues: Vec<_> = revenue_facts
        .iter()
        .filter(|f| f.form.as_deref() == Some("10-K"))
        .take(3)
        .collect();

    for fact in annual_revenues {
        println!(
            "FY{}: ${:.2}B",
            fact.fiscal_year.unwrap_or(0),
            fact.value / 1_000_000_000.0
        );
    }
    println!();

    // Example 6: Calculate key financial ratios
    println!("=== Example 6: Key Financial Ratios ===");
    if let (Some(assets), Some(liabilities), Some(equity)) = (
        doc.get_latest_fact(concepts::balance_sheet::ASSETS),
        doc.get_latest_fact(concepts::balance_sheet::LIABILITIES),
        doc.get_latest_fact(concepts::balance_sheet::STOCKHOLDERS_EQUITY),
    ) {
        let debt_to_equity = liabilities.value / equity.value;
        let equity_to_assets = equity.value / assets.value;

        println!("Debt-to-Equity Ratio: {:.2}", debt_to_equity);
        println!("Equity-to-Assets Ratio: {:.2}", equity_to_assets);
    }
    println!();

    // Example 7: List all available concepts
    println!("=== Example 7: Available Concepts (first 10) ===");
    let concepts = doc.get_concepts();
    for concept in concepts.iter().take(10) {
        println!("  - {}", concept);
    }
    println!("... and {} more\n", concepts.len().saturating_sub(10));

    // Example 8: Operating Cash Flow analysis
    println!("=== Example 8: Operating Cash Flow ===");
    if let Some(ocf) = doc.get_latest_fact(concepts::cash_flow::OPERATING_CASH_FLOW) {
        println!("Operating Cash Flow: ${:.2}B", ocf.value / 1_000_000_000.0);

        // Compare with Net Income if available
        if let Some(ni) = doc.get_latest_fact(concepts::income_statement::NET_INCOME) {
            let cash_conversion = (ocf.value / ni.value) * 100.0;
            println!("Net Income: ${:.2}B", ni.value / 1_000_000_000.0);
            println!("Cash Conversion Rate: {:.1}%", cash_conversion);
        }
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
