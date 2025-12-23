//! Demo of SEC EDGAR filings API.
//!
//! This example demonstrates how to:
//! - Look up a company's CIK from its ticker symbol
//! - Fetch the company's filing history
//! - Get the latest 10-K and 10-Q filings
//!
//! Run with: cargo run --example edgar_filings_demo

use perth_data::edgar::{CikLookup, CompanyFilings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an HTTP client
    let client = reqwest::Client::builder()
        .user_agent("Perth Factor Model/1.0")
        .build()?;

    println!("Fetching CIK lookup table from SEC...");
    let lookup = CikLookup::fetch(&client).await?;

    // Look up Apple's CIK
    let ticker = "AAPL";
    if let Some((cik, name)) = lookup.get_cik(ticker) {
        println!("\n{} ({}):", name, ticker);
        println!("  CIK: {}", cik);

        // Fetch filing history
        println!("\nFetching filing history...");
        let filings = CompanyFilings::fetch(&client, cik).await?;

        // Get latest 10-K
        if let Some(filing) = filings.latest_10k() {
            println!("\nLatest 10-K:");
            println!("  Date: {}", filing.filing_date);
            println!("  Accession: {}", filing.accession_number);
            println!("  URL: {}", filing.document_url(cik));
        }

        // Get latest 10-Q
        if let Some(filing) = filings.latest_10q() {
            println!("\nLatest 10-Q:");
            println!("  Date: {}", filing.filing_date);
            println!("  Accession: {}", filing.accession_number);
            println!("  URL: {}", filing.document_url(cik));
        }

        // Show all 10-K filings
        let all_10k = filings.all_10k();
        println!("\nAll 10-K filings ({} total):", all_10k.len());
        for filing in all_10k.iter().take(5) {
            println!("  {} - {}", filing.filing_date, filing.accession_number);
        }
        if all_10k.len() > 5 {
            println!("  ... and {} more", all_10k.len() - 5);
        }
    } else {
        println!("Ticker {} not found", ticker);
    }

    Ok(())
}
