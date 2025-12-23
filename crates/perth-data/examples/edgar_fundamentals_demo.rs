//! Example demonstrating SEC EDGAR fundamentals data extraction.
//!
//! This example shows how to:
//! 1. Fetch financial statements from SEC EDGAR
//! 2. Extract specific quarterly and annual data
//! 3. Compute factor inputs for investment analysis
//!
//! Note: This requires network access to SEC EDGAR APIs.

use perth_data::edgar::EdgarFundamentalsProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("SEC EDGAR Fundamentals Data Extraction Demo");
    println!("============================================\n");

    // Create the provider
    let provider = EdgarFundamentalsProvider::new();

    // Example 1: Fetch all available financial statements
    println!("Example 1: Fetching all financial statements for AAPL");
    println!("------------------------------------------------------");

    match provider.fetch_financials("AAPL").await {
        Ok(statements) => {
            println!("Found {} financial statements", statements.len());

            // Show the most recent few
            for (i, stmt) in statements.iter().take(5).enumerate() {
                println!(
                    "{}. {} FY{} Q{:?} ending {} - Revenue: ${:.2}M, Net Income: ${:.2}M",
                    i + 1,
                    match stmt.period_type {
                        perth_data::edgar::PeriodType::Quarterly => "10-Q",
                        perth_data::edgar::PeriodType::Annual => "10-K",
                    },
                    stmt.fiscal_year,
                    stmt.fiscal_quarter,
                    stmt.period_end,
                    stmt.revenue.unwrap_or(0.0) / 1_000_000.0,
                    stmt.net_income.unwrap_or(0.0) / 1_000_000.0,
                );
            }
        }
        Err(e) => {
            eprintln!("Error fetching financials: {}", e);
            eprintln!("Note: This is expected if you don't have network access to SEC EDGAR");
        }
    }

    println!();

    // Example 2: Fetch the latest quarterly statement
    println!("Example 2: Fetching latest quarterly statement");
    println!("-----------------------------------------------");

    match provider.fetch_latest_quarterly("MSFT").await {
        Ok(stmt) => {
            println!("Latest Quarterly Statement for MSFT:");
            println!(
                "  Period: {} (FY{} Q{})",
                stmt.period_end,
                stmt.fiscal_year,
                stmt.fiscal_quarter.unwrap_or(0)
            );
            println!(
                "  Revenue: ${:.2}M",
                stmt.revenue.unwrap_or(0.0) / 1_000_000.0
            );
            println!(
                "  Net Income: ${:.2}M",
                stmt.net_income.unwrap_or(0.0) / 1_000_000.0
            );
            println!(
                "  Total Assets: ${:.2}M",
                stmt.total_assets.unwrap_or(0.0) / 1_000_000.0
            );
            println!(
                "  Stockholders Equity: ${:.2}M",
                stmt.stockholders_equity.unwrap_or(0.0) / 1_000_000.0
            );
        }
        Err(e) => {
            eprintln!("Error fetching latest quarterly: {}", e);
        }
    }

    println!();

    // Example 3: Compute factor inputs
    println!("Example 3: Computing factor inputs for investment analysis");
    println!("----------------------------------------------------------");

    match provider.fetch_latest_annual("GOOGL").await {
        Ok(stmt) => {
            // Use a hypothetical current price
            let current_price = 140.0;

            let factors = provider.compute_factor_inputs(&stmt, current_price);

            println!("Factor Inputs for GOOGL (Price: ${:.2}):", current_price);
            if let Some(bvps) = factors.book_value_per_share {
                println!("  Book Value per Share: ${:.2}", bvps);
            }
            if let Some(eps) = factors.earnings_per_share {
                println!("  Earnings per Share: ${:.2}", eps);
            }
            if let Some(pb) = factors.price_to_book {
                println!("  Price to Book: {:.2}", pb);
            }
            if let Some(pe) = factors.price_to_earnings {
                println!("  Price to Earnings: {:.2}", pe);
            }
            if let Some(roe) = factors.roe {
                println!("  Return on Equity: {:.2}%", roe * 100.0);
            }
            if let Some(roa) = factors.roa {
                println!("  Return on Assets: {:.2}%", roa * 100.0);
            }
            if let Some(de) = factors.debt_to_equity {
                println!("  Debt to Equity: {:.2}", de);
            }
            if let Some(cr) = factors.current_ratio {
                println!("  Current Ratio: {:.2}", cr);
            }
        }
        Err(e) => {
            eprintln!("Error computing factors: {}", e);
        }
    }

    println!();
    println!("Demo complete!");

    Ok(())
}
