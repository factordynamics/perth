//! Demonstration of Perth Factor Registry
//!
//! This example shows how to:
//! - List all available factors
//! - Query factors by category
//! - Get factor metadata
//!
//! Run with: cargo run --example factor_registry_demo -p perth-factors

use perth_factors::{
    FactorCategory, FactorInfo, available_factors, factors_by_category, get_factor_info,
    registry::{count_by_category, list_factor_names},
};

fn main() {
    println!("Perth Factor Registry Demo");
    println!("==========================\n");

    // List all available factors
    let all_factors = available_factors();
    println!("Total factors available: {}\n", all_factors.len());

    // Show count by category
    println!("Factors by Category:");
    println!("--------------------");
    let counts = count_by_category();
    for (category, count) in &counts {
        let category_name = match category {
            FactorCategory::Value => "Value",
            FactorCategory::Momentum => "Momentum",
            FactorCategory::Size => "Size",
            FactorCategory::Volatility => "Volatility",
            FactorCategory::Quality => "Quality",
            FactorCategory::Growth => "Growth",
            FactorCategory::Liquidity => "Liquidity",
        };
        println!("  {:15} {:2} factors", category_name, count);
    }

    println!("\n");

    // Show all factors organized by category
    println!("Factor Details by Category:");
    println!("===========================\n");

    print_category_factors(FactorCategory::Value, "Value Factors");
    print_category_factors(FactorCategory::Momentum, "Momentum Factors");
    print_category_factors(FactorCategory::Size, "Size Factors");
    print_category_factors(FactorCategory::Volatility, "Volatility Factors");
    print_category_factors(FactorCategory::Quality, "Quality Factors");
    print_category_factors(FactorCategory::Growth, "Growth Factors");
    print_category_factors(FactorCategory::Liquidity, "Liquidity Factors");

    // Demonstrate factor lookup by name
    println!("\nFactor Lookup Example:");
    println!("----------------------");

    if let Some(beta_info) = get_factor_info("beta") {
        println!("Looking up 'beta':");
        print_factor_details(&beta_info);
    }

    if let Some(roe_info) = get_factor_info("roe") {
        println!("\nLooking up 'roe':");
        print_factor_details(&roe_info);
    }

    // List all factor names
    println!("\nAll Factor Names:");
    println!("-----------------");
    let names = list_factor_names();
    for (i, name) in names.iter().enumerate() {
        if i > 0 && i % 4 == 0 {
            println!();
        }
        print!("  {:25}", name);
    }
    println!("\n");

    println!("Demo complete!");
}

fn print_category_factors(category: FactorCategory, title: &str) {
    let factors = factors_by_category(category);
    println!("{}", title);
    println!("{}", "-".repeat(title.len()));

    for factor in &factors {
        println!("  {} - {}", factor.name, factor.description);
        println!("    Required columns: {:?}", factor.required_columns);
    }
    println!();
}

fn print_factor_details(info: &FactorInfo) {
    println!("  Name:        {}", info.name);
    println!("  Description: {}", info.description);
    println!("  Required:    {:?}", info.required_columns);
}
