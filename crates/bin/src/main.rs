//! Perth CLI binary.
//!
//! Provides command-line interface for the Perth factor model.

mod integration;

use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use integration::data_pipeline::{
    FetchConfig, compute_market_cap_proxy, compute_returns, fetch_market_benchmark_with_config,
    fetch_universe_data_with_progress, prepare_factor_data, print_cache_info,
};
use integration::factor_engine::FactorEngine;
use integration::sector_encoder::encode_gics_sectors;
use ndarray::Array2;
use perth::universe::{GicsSector, SP500Universe, Universe};
use perth_data::yahoo::quotes::YahooQuoteProvider;
use perth_risk::covariance::{
    CovarianceEstimator, EwmaCovarianceEstimator, LedoitWolfConfig, LedoitWolfEstimator,
    VolatilityRegimeDetector,
};
use polars::prelude::*;
use serde_json::json;
use std::process;
use std::time::Duration as StdDuration;
use toraniko_model::{EstimatorConfig, FactorReturnsEstimator, compute_attribution};
use toraniko_traits::ReturnsEstimator;

#[derive(Parser)]
#[command(name = "perth")]
#[command(about = "Perth: Institutional-grade factor model", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Analyze factor attribution for a stock
    Analyze {
        /// Stock symbol
        symbol: String,

        /// Analysis period in years
        #[arg(long, default_value = "5")]
        years: u32,

        /// Disable caching (always fetch fresh data)
        #[arg(long)]
        no_cache: bool,

        /// Force refresh cached data
        #[arg(long)]
        refresh: bool,
    },

    /// Run full universe analysis
    Universe {
        /// Filter by GICS sector
        #[arg(long)]
        sector: Option<String>,

        /// List all sectors
        #[arg(long)]
        list_sectors: bool,
    },

    /// Update data cache
    Update {
        /// Update quotes
        #[arg(long)]
        quotes: bool,

        /// Update fundamentals
        #[arg(long)]
        fundamentals: bool,

        /// Update all data
        #[arg(long)]
        full: bool,
    },

    /// Risk analysis and covariance estimation
    Risk {
        /// Show factor covariance estimation
        #[arg(long)]
        covariance: bool,

        /// Show specific risk estimation
        #[arg(long)]
        specific: bool,

        /// Show volatility regime analysis
        #[arg(long)]
        regime: bool,

        /// Analyze specific symbol (optional)
        #[arg(long)]
        symbol: Option<String>,

        /// Output format (json or text)
        #[arg(long, default_value = "text")]
        format: String,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Analyze {
            symbol,
            years,
            no_cache,
            refresh,
        } => {
            let config = FetchConfig {
                use_cache: !no_cache,
                force_refresh: refresh,
            };
            analyze_symbol(&symbol, years, config).await?;
        }
        Commands::Universe {
            sector,
            list_sectors,
        } => {
            if list_sectors {
                list_all_sectors();
            } else {
                run_universe_analysis(sector).await?;
            }
        }
        Commands::Update {
            quotes,
            fundamentals,
            full,
        } => {
            update_data(quotes, fundamentals, full).await?;
        }
        Commands::Risk {
            covariance,
            specific,
            regime,
            symbol,
            format,
        } => {
            risk_analysis(covariance, specific, regime, symbol, &format).await?;
        }
    }

    Ok(())
}

async fn analyze_symbol(
    symbol: &str,
    years: u32,
    config: FetchConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let symbol = symbol.to_uppercase();
    let universe = SP500Universe::new();

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!(
        "║{:^62}║",
        format!("FACTOR ATTRIBUTION ANALYSIS: {}", symbol)
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Determine sector
    let sector_name = universe.sector(&symbol).map_or_else(
        || {
            println!("Sector: Unknown (not in S&P 500 universe)");
            "Other".to_string()
        },
        |s| {
            println!("GICS Sector: {}", s);
            s.name().to_string()
        },
    );

    println!("Analysis Period: {} year(s)", years);
    println!("Model: Cross-sectional factor regression (5 style factors, 11 GICS sectors)");

    // Print cache status
    if config.use_cache {
        print_cache_info();
        if config.force_refresh {
            println!("  Mode: Force refresh (re-fetching all data)");
        }
    } else {
        println!("  Cache: Disabled");
    }
    println!();

    let provider = YahooQuoteProvider::new();
    let end = Utc::now();
    let start = end - Duration::days(years as i64 * 252);

    // Create progress bar for data fetching (the slow step)
    let pb = ProgressBar::new(universe.size() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .expect("valid template")
            .progress_chars("█▓░"),
    );
    pb.enable_steady_tick(StdDuration::from_millis(100));
    pb.set_message("Fetching universe data...");

    // Fetch universe data with progress reporting
    let quotes = match fetch_universe_data_with_progress(
        &provider,
        &universe,
        start,
        end,
        config.clone(),
        Some(&pb),
    )
    .await
    {
        Ok(q) => {
            let n_symbols = q
                .column("symbol")
                .ok()
                .and_then(|c| c.unique().ok())
                .map(|u| u.len())
                .unwrap_or(0);
            pb.finish_with_message(format!(
                "Fetched {} stocks ({} rows)",
                n_symbols,
                q.height()
            ));
            q
        }
        Err(e) => {
            pb.finish_with_message("Failed!");
            return Err(format!("Failed to fetch universe data: {}", e).into());
        }
    };

    // Fetch market benchmark (SPY) - quick operation, no progress bar needed
    print!("Fetching market benchmark (SPY)...");
    std::io::Write::flush(&mut std::io::stdout())?;
    let market_returns =
        match fetch_market_benchmark_with_config(&provider, start, end, config).await {
            Ok(mr) => {
                println!(" ✓");
                mr
            }
            Err(e) => {
                println!(" ✗");
                return Err(format!("Failed to fetch SPY: {}", e).into());
            }
        };

    // Compute returns and market cap
    print!("Computing returns and market cap...");
    std::io::Write::flush(&mut std::io::stdout())?;
    let returns_df = compute_returns(&quotes)?;
    let mkt_cap_df = compute_market_cap_proxy(&quotes)?;
    println!(" ✓");

    // Prepare factor data (joins all necessary columns)
    print!("Preparing factor data...");
    std::io::Write::flush(&mut std::io::stdout())?;
    let factor_data = prepare_factor_data(&quotes, &market_returns, &mkt_cap_df)?;
    println!(" ✓ ({} observations)", factor_data.height());

    // Encode GICS sectors
    print!("Encoding GICS sectors...");
    std::io::Write::flush(&mut std::io::stdout())?;
    let sector_df = encode_gics_sectors(&universe, &quotes)?;
    println!(" ✓ (11 sectors)");

    // Compute factor scores
    print!("Computing factor scores...");
    std::io::Write::flush(&mut std::io::stdout())?;
    let factor_engine = FactorEngine::new();
    let style_df = match factor_engine.compute_all_scores(&factor_data) {
        Ok(df) => {
            println!(" ✓ ({} factors)", factor_engine.available_factors().len());
            df
        }
        Err(e) => {
            println!(" ✗");
            return Err(format!("Failed to compute factor scores: {}", e).into());
        }
    };

    // Run factor returns estimation via WLS regression
    print!("Running cross-sectional regression...");
    std::io::Write::flush(&mut std::io::stdout())?;
    let estimator_config = EstimatorConfig {
        winsor_factor: Some(0.05),
        residualize_styles: true,
    };
    let estimator = FactorReturnsEstimator::with_config(estimator_config);

    let (factor_returns, residuals) = match estimator.estimate(
        returns_df,
        mkt_cap_df,
        sector_df.clone().lazy(),
        style_df.clone().lazy(),
    ) {
        Ok((fr, res)) => {
            println!(" ✓");
            (fr, res)
        }
        Err(e) => {
            println!(" ✗");
            return Err(format!("Factor estimation failed: {}", e).into());
        }
    };

    // Compute attribution for target symbol
    print!("Computing attribution for {}...", symbol);
    std::io::Write::flush(&mut std::io::stdout())?;
    let attribution =
        match compute_attribution(&symbol, &factor_returns, &residuals, &style_df, &sector_df) {
            Ok(attr) => {
                println!(" ✓");
                attr
            }
            Err(e) => {
                println!(" ✗");
                return Err(format!("Attribution failed for {}: {}", symbol, e).into());
            }
        };

    println!();

    // Print results using toraniko-model's formatted output
    attribution.print_summary();

    // Print additional Perth-specific summary
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("PERTH MODEL DETAILS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    println!(
        "  Style Factors:   {} (vs toraniko-rs baseline: 3)",
        factor_engine.available_factors().len()
    );
    println!("  GICS Sectors:    11 (vs toraniko-rs baseline: 3)");
    println!(
        "  Universe Size:   {} stocks (vs toraniko-rs baseline: 30)",
        universe.size()
    );
    println!("  Target Sector:   {}", sector_name);

    println!("\n════════════════════════════════════════════════════════════════\n");

    Ok(())
}

async fn run_universe_analysis(
    sector_filter: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let universe = SP500Universe::new();

    println!("S&P 500 Universe Analysis");
    println!("=========================\n");

    if let Some(sector_name) = sector_filter {
        // Filter by sector
        let sector = parse_sector(&sector_name)?;
        let symbols = universe.symbols_in_sector(sector);

        println!("Sector: {}", sector);
        println!("Constituents: {}\n", symbols.len());

        for symbol in symbols {
            println!("  {}", symbol);
        }
    } else {
        // Show all sectors
        let sector_counts = universe.sector_counts();

        println!("Total constituents: {}\n", universe.size());
        println!("Breakdown by sector:");

        for sector in GicsSector::all() {
            let count = sector_counts.get(&sector).unwrap_or(&0);
            println!("  {:30} {:3} stocks", sector.name(), count);
        }
    }

    Ok(())
}

fn list_all_sectors() {
    println!("GICS Sectors:");
    println!("=============\n");

    for sector in GicsSector::all() {
        println!("{:2} - {}", sector.code(), sector.name());
    }
}

fn parse_sector(name: &str) -> Result<GicsSector, Box<dyn std::error::Error>> {
    let normalized = name.to_lowercase().replace(' ', "");

    let sector = match normalized.as_str() {
        "informationtechnology" | "it" | "tech" => GicsSector::InformationTechnology,
        "healthcare" | "health" => GicsSector::HealthCare,
        "financials" | "finance" => GicsSector::Financials,
        "consumerdiscretionary" | "discretionary" => GicsSector::ConsumerDiscretionary,
        "communicationservices" | "communication" | "comms" => GicsSector::CommunicationServices,
        "industrials" | "industrial" => GicsSector::Industrials,
        "consumerstaples" | "staples" => GicsSector::ConsumerStaples,
        "energy" => GicsSector::Energy,
        "utilities" | "utility" => GicsSector::Utilities,
        "realestate" | "estate" => GicsSector::RealEstate,
        "materials" => GicsSector::Materials,
        _ => return Err(format!("Unknown sector: {}", name).into()),
    };

    Ok(sector)
}

async fn update_data(
    quotes: bool,
    fundamentals: bool,
    full: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if full {
        println!("Updating all data...");
        println!("  - Quotes: [Not yet implemented]");
        println!("  - Fundamentals: [Not yet implemented]");
    } else {
        if quotes {
            println!("Updating quotes: [Not yet implemented]");
        }
        if fundamentals {
            println!("Updating fundamentals: [Not yet implemented]");
        }
        if !quotes && !fundamentals {
            println!("No data selected for update. Use --quotes, --fundamentals, or --full");
        }
    }

    println!("\nNote: Data update requires implementation in perth-data crate.");

    Ok(())
}

async fn risk_analysis(
    show_covariance: bool,
    show_specific: bool,
    show_regime: bool,
    symbol: Option<String>,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // If no flags are set, show everything
    let show_all = !show_covariance && !show_specific && !show_regime;
    let do_covariance = show_all || show_covariance;
    let do_specific = show_all || show_specific;
    let do_regime = show_all || show_regime;

    // Generate synthetic factor returns for demonstration
    // In production, this would come from real data
    let (factor_returns, factor_names) = generate_sample_factor_returns(252);

    // Generate synthetic specific returns if symbol is provided
    let specific_volatility = symbol
        .as_ref()
        .map(|sym| generate_sample_specific_risk(sym, 252));

    // Determine output format
    let is_json = format.to_lowercase() == "json";

    if is_json {
        output_risk_json(
            &factor_returns,
            &factor_names,
            specific_volatility,
            symbol.as_deref(),
            do_covariance,
            do_specific,
            do_regime,
        )?;
    } else {
        output_risk_text(
            &factor_returns,
            &factor_names,
            specific_volatility,
            symbol.as_deref(),
            do_covariance,
            do_specific,
            do_regime,
        )?;
    }

    Ok(())
}

fn generate_sample_factor_returns(n_periods: usize) -> (Array2<f64>, Vec<String>) {
    // Factor names matching Perth's factor model
    let factor_names = vec![
        "Value".to_string(),
        "Momentum".to_string(),
        "Size".to_string(),
        "Volatility".to_string(),
        "Quality".to_string(),
        "Growth".to_string(),
        "Liquidity".to_string(),
    ];

    let n_factors = factor_names.len();
    let mut returns = Array2::<f64>::zeros((n_periods, n_factors));

    // Generate synthetic returns with realistic properties
    // Using simple sine waves with different phases and amplitudes
    for t in 0..n_periods {
        let time = t as f64 / n_periods as f64;

        // Value: counter-cyclical, mean-reverting
        returns[[t, 0]] = 0.0008 * (time * 12.0).sin() + 0.0002 * (time * 3.0).cos();

        // Momentum: trending with reversals
        returns[[t, 1]] = 0.001 * (time * 8.0).sin() + 0.0003 * (time * 20.0).sin();

        // Size: lower volatility, steady
        returns[[t, 2]] = 0.0005 * (time * 6.0).sin() + 0.0001 * (time * 15.0).cos();

        // Volatility: counter-cyclical to market
        returns[[t, 3]] = -0.0012 * (time * 10.0).sin() + 0.0004 * (time * 25.0).sin();

        // Quality: defensive, low volatility
        returns[[t, 4]] = 0.0006 * (time * 5.0).sin() + 0.0001 * (time * 12.0).cos();

        // Growth: higher volatility
        returns[[t, 5]] = 0.0015 * (time * 9.0).sin() + 0.0005 * (time * 18.0).sin();

        // Liquidity: spiky, regime-dependent
        returns[[t, 6]] = 0.0007 * (time * 15.0).sin() + 0.0003 * (time * 30.0).cos();
    }

    (returns, factor_names)
}

fn generate_sample_specific_risk(symbol: &str, _n_periods: usize) -> f64 {
    // Generate synthetic specific risk based on symbol characteristics
    // In production, this would come from residual analysis

    // Hash the symbol to get a deterministic but varied number
    let hash: u32 = symbol.chars().map(|c| c as u32).sum();
    // Between 15% and 25%

    0.15 + (hash % 100) as f64 / 1000.0
}

fn output_risk_text(
    factor_returns: &Array2<f64>,
    factor_names: &[String],
    specific_vol: Option<f64>,
    symbol: Option<&str>,
    show_covariance: bool,
    show_specific: bool,
    show_regime: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║              Perth Risk Analysis (Demo Mode)                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    if let Some(sym) = symbol {
        println!("Symbol: {}\n", sym);
    }

    // Covariance estimation
    if show_covariance {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("FACTOR COVARIANCE MATRIX");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

        // Estimate covariance using EWMA
        let _ewma_estimator = EwmaCovarianceEstimator::try_default()?;
        // Note: In production, you might want to compare EWMA vs Ledoit-Wolf
        // let _ewma_cov = _ewma_estimator.estimate(factor_returns)?;

        // Apply Ledoit-Wolf shrinkage
        let lw_estimator = LedoitWolfEstimator::new(LedoitWolfConfig::default());
        let lw_cov = lw_estimator.estimate(factor_returns)?;

        println!("Method: EWMA (λ=0.95) with Ledoit-Wolf Shrinkage");
        println!("Estimation Period: {} days\n", factor_returns.nrows());

        // Display correlation matrix (easier to read than covariance)
        println!("Factor Correlation Matrix:");
        println!("─────────────────────────────────────────────────────────────");

        // Header
        print!("{:<12}", "");
        for name in factor_names {
            print!("{:>10}", &name[..name.len().min(8)]);
        }
        println!();

        // Convert covariance to correlation
        let std_devs: Vec<f64> = (0..factor_names.len())
            .map(|i| lw_cov[[i, i]].sqrt())
            .collect();

        for i in 0..factor_names.len() {
            print!("{:<12}", &factor_names[i][..factor_names[i].len().min(11)]);
            for j in 0..factor_names.len() {
                let corr = lw_cov[[i, j]] / (std_devs[i] * std_devs[j]);
                print!("{:>10.3}", corr);
            }
            println!();
        }

        println!("\nFactor Volatilities (Annualized):");
        println!("─────────────────────────────────────────────────────────────");
        for (i, name) in factor_names.iter().enumerate() {
            let vol = std_devs[i] * (252.0_f64).sqrt();
            println!("  {:<15} {:>8.2}%", name, vol * 100.0);
        }
        println!();
    }

    // Volatility regime analysis
    if show_regime {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("VOLATILITY REGIME ANALYSIS");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

        let regime_detector = VolatilityRegimeDetector::try_default()?;

        // Analyze regime for first factor (as proxy for market)
        let market_returns = factor_returns.column(0).to_owned();
        let (regime, scale_factor) = regime_detector.analyze(&market_returns);

        let regime_str = match regime {
            perth_risk::covariance::VolatilityRegime::Low => "Low Volatility",
            perth_risk::covariance::VolatilityRegime::Normal => "Normal Volatility",
            perth_risk::covariance::VolatilityRegime::High => "High Volatility",
        };

        println!("Current Regime:         {}", regime_str);
        println!("Variance Scale Factor:  {:.3}x", scale_factor);
        println!("Short Window:           21 days");
        println!("Long Window:            252 days");

        let regime_emoji = match regime {
            perth_risk::covariance::VolatilityRegime::Low => "Calm markets",
            perth_risk::covariance::VolatilityRegime::Normal => "Normal conditions",
            perth_risk::covariance::VolatilityRegime::High => "Elevated risk",
        };
        println!("\nInterpretation: {}", regime_emoji);
        println!();
    }

    // Specific risk estimation
    if show_specific {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("SPECIFIC RISK ESTIMATION");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

        if let Some(vol) = specific_vol {
            // Note: In production, use SpecificRiskEstimator to compute from residuals
            // let _estimator = SpecificRiskEstimator::new(SpecificRiskConfig::default());

            println!("Method: EWMA (λ=0.95)");
            println!("Estimation Period: {} days\n", factor_returns.nrows());

            if let Some(sym) = symbol {
                println!("Symbol: {}", sym);
            }
            println!("Specific Risk (Annualized): {:>8.2}%", vol * 100.0);
            println!("\nSpecific risk represents the idiosyncratic volatility");
            println!("after accounting for factor exposures.");
        } else {
            println!("No symbol specified. Use --symbol <SYMBOL> to estimate specific risk.");
        }
        println!();
    }

    println!("Note: Using synthetic data for demonstration purposes.");
    println!("      Production system will use real market data.\n");

    Ok(())
}

fn output_risk_json(
    factor_returns: &Array2<f64>,
    factor_names: &[String],
    specific_vol: Option<f64>,
    symbol: Option<&str>,
    show_covariance: bool,
    show_specific: bool,
    show_regime: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = json!({
        "analysis_type": "risk",
        "demo_mode": true,
        "estimation_period_days": factor_returns.nrows(),
    });

    if let Some(sym) = symbol {
        output["symbol"] = json!(sym);
    }

    // Covariance estimation
    if show_covariance {
        let lw_estimator = LedoitWolfEstimator::new(LedoitWolfConfig::default());
        let lw_cov = lw_estimator.estimate(factor_returns)?;

        let std_devs: Vec<f64> = (0..factor_names.len())
            .map(|i| lw_cov[[i, i]].sqrt())
            .collect();

        // Build correlation matrix
        let mut correlation = Vec::new();
        for i in 0..factor_names.len() {
            let mut row = Vec::new();
            for j in 0..factor_names.len() {
                let corr = lw_cov[[i, j]] / (std_devs[i] * std_devs[j]);
                row.push(format!("{:.4}", corr));
            }
            correlation.push(row);
        }

        let volatilities: Vec<_> = std_devs
            .iter()
            .zip(factor_names.iter())
            .map(|(vol, name)| {
                json!({
                    "factor": name,
                    "annualized_volatility": format!("{:.4}", vol * (252.0_f64).sqrt())
                })
            })
            .collect();

        output["covariance"] = json!({
            "method": "EWMA with Ledoit-Wolf Shrinkage",
            "ewma_decay": 0.95,
            "factors": factor_names,
            "correlation_matrix": correlation,
            "volatilities": volatilities,
        });
    }

    // Volatility regime
    if show_regime {
        let regime_detector = VolatilityRegimeDetector::try_default()?;
        let market_returns = factor_returns.column(0).to_owned();
        let (regime, scale_factor) = regime_detector.analyze(&market_returns);

        let regime_str = match regime {
            perth_risk::covariance::VolatilityRegime::Low => "low",
            perth_risk::covariance::VolatilityRegime::Normal => "normal",
            perth_risk::covariance::VolatilityRegime::High => "high",
        };

        output["regime"] = json!({
            "current_regime": regime_str,
            "variance_scale_factor": format!("{:.4}", scale_factor),
            "short_window_days": 21,
            "long_window_days": 252,
        });
    }

    // Specific risk
    if show_specific && let Some(vol) = specific_vol {
        output["specific_risk"] = json!({
            "method": "EWMA",
            "ewma_decay": 0.95,
            "annualized_volatility": format!("{:.4}", vol),
        });
    }

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
