//! Comprehensive demonstration of perth-risk crate functionality
//!
//! This example showcases all major components of the Perth risk model:
//! - EWMA covariance estimation
//! - Ledoit-Wolf shrinkage
//! - Newey-West HAC adjustment
//! - Volatility regime detection
//! - Bayesian specific risk estimation
//! - Positive definiteness enforcement

use ndarray::{Array1, Array2};
use perth_risk::covariance::ewma::EwmaConfig;
use perth_risk::covariance::{
    CovarianceEstimator, EwmaCovarianceEstimator, LedoitWolfConfig, LedoitWolfEstimator,
    NeweyWestConfig, NeweyWestEstimator, PositiveDefiniteConfig, ShrinkageTarget,
    VolatilityRegimeConfig, VolatilityRegimeDetector,
};
use perth_risk::specific_risk::{BayesianShrinkageConfig, BayesianSpecificRisk};

fn main() {
    println!("==========================================================");
    println!("          Perth Risk Model - Comprehensive Demo");
    println!("==========================================================\n");

    // Demo 1: EWMA Covariance Estimation
    demo_ewma_covariance();

    // Demo 2: Ledoit-Wolf Shrinkage
    demo_ledoit_wolf_shrinkage();

    // Demo 3: Newey-West HAC Adjustment
    demo_newey_west_hac();

    // Demo 4: Volatility Regime Detection
    demo_volatility_regime();

    // Demo 5: Specific Risk with Bayesian Shrinkage
    demo_bayesian_specific_risk();

    // Demo 6: Positive Definiteness Enforcement
    demo_positive_definite_enforcement();

    println!("==========================================================");
    println!("                    Demo Complete!");
    println!("==========================================================");
}

/// Demo 1: EWMA Covariance Estimation
fn demo_ewma_covariance() {
    println!("----------------------------------------------------------");
    println!("Demo 1: EWMA Covariance Estimation");
    println!("----------------------------------------------------------");

    // Create deterministic factor returns for reproducibility
    // 3 factors over 100 days with realistic daily returns (±0.01 to ±0.02)
    let n_periods = 100;
    let n_factors = 3;
    let mut factor_returns_data = Vec::with_capacity(n_periods * n_factors);

    println!(
        "Creating sample factor returns data ({} periods, {} factors)...",
        n_periods, n_factors
    );

    for t in 0..n_periods {
        let t_f = t as f64;
        // Factor 1: Trending with noise
        let r1 = 0.0005 + 0.015 * (t_f * 0.1).sin();
        // Factor 2: Mean-reverting
        let r2 = 0.012 * (t_f * 0.2).cos();
        // Factor 3: Volatile
        let r3 = 0.018 * (t_f * 0.15).sin() * (t_f * 0.3).cos();

        factor_returns_data.push(r1);
        factor_returns_data.push(r2);
        factor_returns_data.push(r3);
    }

    let factor_returns =
        Array2::from_shape_vec((n_periods, n_factors), factor_returns_data).unwrap();

    // Configure EWMA with λ = 0.94
    let ewma_config = EwmaConfig {
        decay: 0.94,
        min_observations: 60,
        bias_correction: true,
    };

    println!("\nEWMA Configuration:");
    println!("  Decay factor (λ): {}", ewma_config.decay);
    println!("  Min observations: {}", ewma_config.min_observations);
    println!("  Bias correction: {}", ewma_config.bias_correction);

    let ewma_estimator = EwmaCovarianceEstimator::new(ewma_config).unwrap();
    println!("  Half-life: {:.2} periods", ewma_estimator.half_life());

    // Estimate covariance
    let ewma_cov = ewma_estimator.estimate(&factor_returns).unwrap();

    println!("\nEWMA Covariance Matrix:");
    for i in 0..n_factors {
        print!("  [");
        for j in 0..n_factors {
            print!(" {:8.6}", ewma_cov[[i, j]]);
        }
        println!(" ]");
    }

    println!("\nEWMA Volatilities (annualized, sqrt(252)):");
    for i in 0..n_factors {
        let vol = (ewma_cov[[i, i]] * 252.0).sqrt();
        println!("  Factor {}: {:.2}%", i + 1, vol * 100.0);
    }

    println!("\nEWMA Correlations:");
    for i in 0..n_factors {
        for j in (i + 1)..n_factors {
            let corr = ewma_cov[[i, j]] / (ewma_cov[[i, i]] * ewma_cov[[j, j]]).sqrt();
            println!("  Factor {} vs Factor {}: {:.4}", i + 1, j + 1, corr);
        }
    }
    println!();
}

/// Demo 2: Ledoit-Wolf Shrinkage
fn demo_ledoit_wolf_shrinkage() {
    println!("----------------------------------------------------------");
    println!("Demo 2: Ledoit-Wolf Shrinkage Covariance");
    println!("----------------------------------------------------------");

    // Create sample data with limited observations relative to dimensionality
    let n_periods = 80;
    let n_factors = 5;
    let mut factor_returns_data = Vec::with_capacity(n_periods * n_factors);

    println!(
        "Creating factor returns ({} periods, {} factors)...",
        n_periods, n_factors
    );
    println!(
        "Note: Limited observations relative to dimensionality to demonstrate shrinkage benefits"
    );

    for t in 0..n_periods {
        let t_f = t as f64;
        for f in 0..n_factors {
            let phase = t_f * 0.1 + f as f64;
            let ret = 0.01 * phase.sin() + 0.005 * (phase * 1.5).cos();
            factor_returns_data.push(ret);
        }
    }

    let factor_returns =
        Array2::from_shape_vec((n_periods, n_factors), factor_returns_data).unwrap();

    // Configure Ledoit-Wolf with different shrinkage targets
    let lw_config = LedoitWolfConfig {
        min_observations: 2,
        target: ShrinkageTarget::Identity,
        center: true,
    };

    println!("\nLedoit-Wolf Configuration:");
    println!("  Shrinkage target: Identity matrix");
    println!("  Centering: {}", lw_config.center);

    let lw_estimator = LedoitWolfEstimator::new(lw_config);

    // Get shrinkage intensity
    let delta = lw_estimator
        .get_shrinkage_intensity(&factor_returns)
        .unwrap();
    println!("\n  Optimal shrinkage intensity (δ*): {:.4}", delta);
    println!(
        "  Interpretation: {:.1}% toward target, {:.1}% toward sample covariance",
        delta * 100.0,
        (1.0 - delta) * 100.0
    );

    // Estimate with shrinkage
    let lw_cov = lw_estimator.estimate(&factor_returns).unwrap();

    println!("\nLedoit-Wolf Covariance Matrix (first 3x3 block):");
    for i in 0..3 {
        print!("  [");
        for j in 0..3 {
            print!(" {:8.6}", lw_cov[[i, j]]);
        }
        println!(" ]");
    }

    // Compare with sample covariance
    let centered_returns = {
        let means = factor_returns.mean_axis(ndarray::Axis(0)).unwrap();
        &factor_returns - &means.insert_axis(ndarray::Axis(0))
    };
    let sample_cov = centered_returns.t().dot(&centered_returns) / n_periods as f64;

    println!("\nComparison - Diagonal Elements (Variances):");
    println!("  Factor | Sample Cov | LW Shrunk | Difference");
    println!("  -------|------------|-----------|------------");
    for i in 0..n_factors {
        let diff = lw_cov[[i, i]] - sample_cov[[i, i]];
        println!(
            "  {:6} | {:10.7} | {:9.7} | {:+9.7}",
            i + 1,
            sample_cov[[i, i]],
            lw_cov[[i, i]],
            diff
        );
    }
    println!();
}

/// Demo 3: Newey-West HAC Adjustment
fn demo_newey_west_hac() {
    println!("----------------------------------------------------------");
    println!("Demo 3: Newey-West HAC Covariance Estimation");
    println!("----------------------------------------------------------");

    // Create data with autocorrelation
    let n_periods = 120;
    let n_factors = 3;
    let mut factor_returns_data = Vec::with_capacity(n_periods * n_factors);

    println!(
        "Creating factor returns with autocorrelation ({} periods, {} factors)...",
        n_periods, n_factors
    );

    // Factor 1: Strong positive autocorrelation
    let mut r1_prev = 0.0;
    for t in 0..n_periods {
        let shock = 0.01 * ((t as f64) * 0.2).sin();
        let r1 = 0.6 * r1_prev + shock; // AR(1) with ρ=0.6
        factor_returns_data.push(r1);
        r1_prev = r1;
    }

    // Factor 2: Moderate autocorrelation
    let mut r2_prev = 0.0;
    for t in 0..n_periods {
        let shock = 0.012 * ((t as f64) * 0.15).cos();
        let r2 = 0.3 * r2_prev + shock; // AR(1) with ρ=0.3
        factor_returns_data.push(r2);
        r2_prev = r2;
    }

    // Factor 3: Weak autocorrelation
    for t in 0..n_periods {
        let r3 = 0.015 * ((t as f64) * 0.1).sin() * ((t as f64) * 0.25).cos();
        factor_returns_data.push(r3);
    }

    // Reshape to (n_periods, n_factors)
    let mut reshaped_data = vec![0.0; n_periods * n_factors];
    for t in 0..n_periods {
        for f in 0..n_factors {
            reshaped_data[t * n_factors + f] = factor_returns_data[f * n_periods + t];
        }
    }

    let factor_returns = Array2::from_shape_vec((n_periods, n_factors), reshaped_data).unwrap();

    // Configure Newey-West
    let nw_config = NeweyWestConfig {
        min_observations: 60,
        lags: None, // Automatic lag selection
        prewhiten: false,
    };

    println!("\nNewey-West Configuration:");
    println!("  Min observations: {}", nw_config.min_observations);
    println!("  Lag selection: Automatic (Newey-West rule)");

    let nw_estimator = NeweyWestEstimator::new(nw_config);

    // Compute optimal lags
    let optimal_lags = {
        let t = n_periods as f64;
        let lags = 4.0 * (t / 100.0).powf(2.0 / 9.0);
        lags.ceil() as usize
    };
    println!("  Optimal lags (L): {}", optimal_lags);

    // Estimate HAC covariance
    let nw_cov = nw_estimator.estimate(&factor_returns).unwrap();

    println!("\nNewey-West HAC Covariance Matrix:");
    for i in 0..n_factors {
        print!("  [");
        for j in 0..n_factors {
            print!(" {:8.6}", nw_cov[[i, j]]);
        }
        println!(" ]");
    }

    println!("\nHAC-Adjusted Volatilities (annualized):");
    for i in 0..n_factors {
        let vol = (nw_cov[[i, i]] * 252.0).sqrt();
        println!("  Factor {}: {:.2}%", i + 1, vol * 100.0);
    }

    println!("\nNote: HAC adjustment accounts for serial correlation in the time series.");
    println!(
        "      This typically increases variance estimates for positively correlated series.\n"
    );
}

/// Demo 4: Volatility Regime Detection
fn demo_volatility_regime() {
    println!("----------------------------------------------------------");
    println!("Demo 4: Volatility Regime Detection");
    println!("----------------------------------------------------------");

    // Create returns with changing volatility regimes
    let n_periods = 300;
    let mut returns_data = Vec::with_capacity(n_periods);

    println!(
        "Creating return series with varying volatility ({} periods)...",
        n_periods
    );
    println!("  Periods   0-100: Normal volatility (σ ≈ 1.5%)");
    println!("  Periods 100-200: Low volatility (σ ≈ 0.8%)");
    println!("  Periods 200-300: High volatility (σ ≈ 3.0%)");

    // Normal volatility period
    for t in 0..100 {
        let ret = 0.015 * ((t as f64) * 0.1).sin();
        returns_data.push(ret);
    }

    // Low volatility period
    for t in 100..200 {
        let ret = 0.008 * ((t as f64) * 0.1).sin();
        returns_data.push(ret);
    }

    // High volatility period (stress scenario)
    for t in 200..300 {
        let ret = 0.030 * ((t as f64) * 0.1).sin() * ((t as f64) * 0.2).cos();
        returns_data.push(ret);
    }

    let returns = Array1::from_vec(returns_data);

    // Configure regime detector
    let regime_config = VolatilityRegimeConfig {
        short_window: 21, // ~1 month
        long_window: 252, // ~1 year
        low_vol_threshold: 0.75,
        high_vol_threshold: 1.5,
        max_scale: 3.0,
    };

    println!("\nVolatility Regime Configuration:");
    println!("  Short window: {} days", regime_config.short_window);
    println!("  Long window: {} days", regime_config.long_window);
    println!(
        "  Low vol threshold: {:.2}",
        regime_config.low_vol_threshold
    );
    println!(
        "  High vol threshold: {:.2}",
        regime_config.high_vol_threshold
    );
    println!("  Max scale factor: {:.2}", regime_config.max_scale);

    let regime_detector = VolatilityRegimeDetector::new(regime_config).unwrap();

    // Detect regime at the end of the series
    let (regime, scale_factor) = regime_detector.analyze(&returns);

    println!("\nDetected Volatility Regime: {:?}", regime);
    println!(
        "  Interpretation: Current volatility is {} relative to historical average",
        match regime {
            perth_risk::covariance::VolatilityRegime::Low => "LOW",
            perth_risk::covariance::VolatilityRegime::Normal => "NORMAL",
            perth_risk::covariance::VolatilityRegime::High => "HIGH",
        }
    );

    println!("\nVolatility Scale Factor: {:.4}", scale_factor);
    println!(
        "  Interpretation: Covariance matrix should be scaled by {:.4}",
        scale_factor
    );
    println!(
        "  This corresponds to volatility scaling of {:.4}",
        scale_factor.sqrt()
    );

    // Example: Apply scaling to a sample covariance matrix
    let sample_cov =
        Array2::from_shape_vec((2, 2), vec![0.0002, 0.00015, 0.00015, 0.00025]).unwrap();

    println!("\nExample - Scaling a Sample Covariance Matrix:");
    println!("  Original covariance:");
    for i in 0..2 {
        print!("    [");
        for j in 0..2 {
            print!(" {:9.6}", sample_cov[[i, j]]);
        }
        println!(" ]");
    }

    let scaled_cov = regime_detector.scale_covariance(&sample_cov, &returns);

    println!("  Regime-adjusted covariance:");
    for i in 0..2 {
        print!("    [");
        for j in 0..2 {
            print!(" {:9.6}", scaled_cov[[i, j]]);
        }
        println!(" ]");
    }

    println!();
}

/// Demo 5: Specific Risk with Bayesian Shrinkage
fn demo_bayesian_specific_risk() {
    println!("----------------------------------------------------------");
    println!("Demo 5: Specific Risk Estimation with Bayesian Shrinkage");
    println!("----------------------------------------------------------");

    // Create residual returns for multiple securities in different sectors
    let n_periods = 120;
    let n_securities = 6;

    println!(
        "Creating residual returns for {} securities over {} periods...",
        n_securities, n_periods
    );
    println!("  Securities 0-1: Technology sector (high volatility)");
    println!("  Securities 2-3: Utilities sector (low volatility)");
    println!("  Securities 4-5: Finance sector (medium volatility)");

    let mut residuals_data = Vec::with_capacity(n_periods * n_securities);

    for t in 0..n_periods {
        let t_f = t as f64;

        // Technology sector: High specific risk
        residuals_data.push(0.025 * (t_f * 0.15).sin() * (t_f * 0.3).cos());
        residuals_data.push(0.022 * (t_f * 0.12).cos() * (t_f * 0.25).sin());

        // Utilities sector: Low specific risk
        residuals_data.push(0.008 * (t_f * 0.1).sin());
        residuals_data.push(0.009 * (t_f * 0.11).cos());

        // Finance sector: Medium specific risk
        residuals_data.push(0.015 * (t_f * 0.13).sin() * (t_f * 0.2).cos());
        residuals_data.push(0.016 * (t_f * 0.14).cos() * (t_f * 0.22).sin());
    }

    let residuals = Array2::from_shape_vec((n_periods, n_securities), residuals_data).unwrap();

    // Sector assignments (0=Tech, 1=Utilities, 2=Finance)
    let sector_assignments = vec![0, 0, 1, 1, 2, 2];

    // Configure Bayesian shrinkage
    let shrinkage_config = BayesianShrinkageConfig {
        shrinkage_strength: 60.0, // κ = 60
        default_prior_vol: 0.30,
        annualization_factor: (252.0_f64).sqrt(),
        min_observations: 20,
    };

    println!("\nBayesian Shrinkage Configuration:");
    println!(
        "  Shrinkage strength (κ): {}",
        shrinkage_config.shrinkage_strength
    );
    println!(
        "  Default prior vol: {:.1}%",
        shrinkage_config.default_prior_vol * 100.0
    );
    println!("  Min observations: {}", shrinkage_config.min_observations);

    let shrinkage_weight =
        n_periods as f64 / (n_periods as f64 + shrinkage_config.shrinkage_strength);
    println!(
        "  Effective weight on individual estimates: {:.3}",
        shrinkage_weight
    );
    println!(
        "  Effective weight on sector priors: {:.3}",
        1.0 - shrinkage_weight
    );

    let bayesian_estimator = BayesianSpecificRisk::new(shrinkage_config);

    // Estimate with Bayesian shrinkage
    let shrunk_vols = bayesian_estimator
        .estimate_batch(&residuals, &sector_assignments)
        .unwrap();

    // For comparison, compute raw individual volatilities
    println!("\nSpecific Risk Estimates (Annualized Volatility):");
    println!("  Security | Sector      | Individual | Shrunk   | Change");
    println!("  ---------|-------------|------------|----------|--------");

    let sector_names = ["Tech", "Utilities", "Finance"];

    for i in 0..n_securities {
        let security_residuals = residuals.column(i).to_owned();
        let mean = security_residuals.mean().unwrap_or(0.0);
        let variance = security_residuals
            .iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>()
            / ((n_periods - 1) as f64);
        let individual_vol = variance.sqrt() * (252.0_f64).sqrt();

        let change = (shrunk_vols[i] / individual_vol - 1.0) * 100.0;

        println!(
            "  {:8} | {:11} | {:8.2}% | {:7.2}% | {:+6.1}%",
            i,
            sector_names[sector_assignments[i]],
            individual_vol * 100.0,
            shrunk_vols[i] * 100.0,
            change
        );
    }

    // Compute and display sector averages
    println!("\nSector-Level Summary:");
    for (sector_id, sector_name) in sector_names.iter().enumerate() {
        let sector_vols: Vec<f64> = shrunk_vols
            .iter()
            .zip(sector_assignments.iter())
            .filter(|(_, s)| **s == sector_id)
            .map(|(&v, _)| v)
            .collect();

        let avg_vol = sector_vols.iter().sum::<f64>() / sector_vols.len() as f64;
        println!(
            "  {}: Average shrunk volatility = {:.2}%",
            sector_name,
            avg_vol * 100.0
        );
    }

    println!("\nInterpretation: Bayesian shrinkage pulls extreme individual estimates");
    println!("                toward sector averages, improving robustness.\n");
}

/// Demo 6: Positive Definiteness Enforcement
fn demo_positive_definite_enforcement() {
    println!("----------------------------------------------------------");
    println!("Demo 6: Positive Definiteness Enforcement");
    println!("----------------------------------------------------------");

    // Create a matrix that is NOT positive definite
    // This can happen due to numerical errors or when correlation > 1
    println!("Creating a non-positive-definite matrix...");
    println!("(Such matrices can arise from numerical errors or estimation issues)");

    // Example: [[1, 0.9], [0.9, 1]] is valid, but [[1, 1.5], [1.5, 1]] is not
    // We'll create one with a negative eigenvalue
    let non_pd_matrix = Array2::from_shape_vec(
        (3, 3),
        vec![
            1.0, 0.95, 0.90, 0.95, 1.0, 0.92, 0.90, 0.92, 0.5, // This makes it nearly singular
        ],
    )
    .unwrap();

    println!("\nOriginal Matrix:");
    for i in 0..3 {
        print!("  [");
        for j in 0..3 {
            print!(" {:7.4}", non_pd_matrix[[i, j]]);
        }
        println!(" ]");
    }

    // Check if it's positive definite
    let is_pd = perth_risk::covariance::is_positive_definite(&non_pd_matrix);
    println!("\nIs positive definite? {}", is_pd);

    // Compute condition number
    let cond_num = perth_risk::covariance::condition_number(&non_pd_matrix);
    println!("Condition number: {:.2}", cond_num);
    if cond_num > 100.0 {
        println!("  Warning: Matrix is ill-conditioned!");
    }

    // Enforce positive definiteness
    println!("\nApplying eigenvalue clipping to enforce positive definiteness...");

    let pd_config = PositiveDefiniteConfig {
        min_eigenvalue: 1e-8,
        preserve_trace: true,
    };

    println!("  Min eigenvalue threshold: {}", pd_config.min_eigenvalue);
    println!("  Preserve trace: {}", pd_config.preserve_trace);

    let fixed_matrix =
        perth_risk::covariance::enforce_positive_definite(&non_pd_matrix, &pd_config).unwrap();

    println!("\nFixed Matrix (Positive Definite):");
    for i in 0..3 {
        print!("  [");
        for j in 0..3 {
            print!(" {:7.4}", fixed_matrix[[i, j]]);
        }
        println!(" ]");
    }

    // Verify it's now positive definite
    let is_pd_now = perth_risk::covariance::is_positive_definite(&fixed_matrix);
    println!("\nIs positive definite now? {}", is_pd_now);

    let new_cond_num = perth_risk::covariance::condition_number(&fixed_matrix);
    println!("New condition number: {:.2}", new_cond_num);

    // Show the change
    println!("\nElement-wise Changes:");
    println!("  Position | Original | Fixed    | Difference");
    println!("  ---------|----------|----------|------------");
    for i in 0..3 {
        for j in 0..3 {
            let diff = fixed_matrix[[i, j]] - non_pd_matrix[[i, j]];
            println!(
                "  [{}, {}]    | {:8.5} | {:8.5} | {:+9.6}",
                i,
                j,
                non_pd_matrix[[i, j]],
                fixed_matrix[[i, j]],
                diff
            );
        }
    }

    println!("\nInterpretation: Eigenvalue clipping ensures numerical stability");
    println!("                while minimally perturbing the original matrix.\n");
}
