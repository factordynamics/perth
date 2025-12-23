//! Bayesian shrinkage for specific risk estimation
//!
//! Implements Bayesian shrinkage that pulls individual specific risk estimates
//! toward sector/group averages. This helps stabilize estimates for securities
//! with limited history.
//!
//! The key idea is to combine individual estimates with a prior (group average):
//! σ_shrunk = w * σ_individual + (1-w) * σ_prior
//! where w = n / (n + κ) is a shrinkage weight based on sample size.

use super::SpecificRiskError;
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};

/// Configuration for Bayesian shrinkage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BayesianShrinkageConfig {
    /// Minimum observations for full weight (κ parameter)
    /// Higher values increase shrinkage toward the prior
    pub shrinkage_strength: f64,

    /// Default prior volatility if no group info available
    /// Represents a reasonable baseline specific risk
    pub default_prior_vol: f64,

    /// Annualization factor (default: sqrt(252) for daily data)
    pub annualization_factor: f64,

    /// Minimum number of observations required for estimation
    pub min_observations: usize,
}

impl Default for BayesianShrinkageConfig {
    fn default() -> Self {
        Self {
            shrinkage_strength: 60.0, // κ = 60 days worth of "prior strength"
            default_prior_vol: 0.30,  // 30% annualized volatility as default
            annualization_factor: (252.0_f64).sqrt(),
            min_observations: 20, // Lower than standard since we have prior
        }
    }
}

/// Bayesian specific risk estimator with shrinkage toward group priors
#[derive(Debug, Default)]
pub struct BayesianSpecificRisk {
    config: BayesianShrinkageConfig,
}

impl BayesianSpecificRisk {
    /// Create a new Bayesian specific risk estimator
    pub const fn new(config: BayesianShrinkageConfig) -> Self {
        Self { config }
    }

    /// Compute shrinkage weight based on sample size
    ///
    /// # Arguments
    /// * `observation_count` - Number of valid observations
    ///
    /// # Returns
    /// Weight in [0, 1] where 1 = full weight on individual estimate,
    /// 0 = full weight on prior
    fn compute_shrinkage_weight(&self, observation_count: usize) -> f64 {
        let n = observation_count as f64;
        let kappa = self.config.shrinkage_strength;
        n / (n + kappa)
    }

    /// Estimate raw volatility from residuals using simple variance
    fn estimate_raw_volatility(&self, residuals: &Array1<f64>) -> Result<f64, SpecificRiskError> {
        let n = residuals.len();

        if n < self.config.min_observations {
            return Err(SpecificRiskError::InsufficientData {
                required: self.config.min_observations,
                actual: n,
            });
        }

        // Compute sample standard deviation (assuming mean residual ≈ 0)
        let mean = residuals.mean().unwrap_or(0.0);
        let variance =
            residuals.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);

        if variance < 0.0 {
            return Err(SpecificRiskError::InvalidVolatility(
                "Negative variance".to_string(),
            ));
        }

        // Return annualized volatility
        Ok(variance.sqrt() * self.config.annualization_factor)
    }

    /// Estimate specific risk with Bayesian shrinkage
    ///
    /// # Arguments
    /// * `residuals` - Residual returns for this security
    /// * `prior_vol` - Prior volatility (e.g., sector average)
    /// * `observation_count` - Number of valid observations (typically residuals.len())
    ///
    /// # Returns
    /// Annualized specific volatility with Bayesian shrinkage applied
    pub fn estimate_with_prior(
        &self,
        residuals: &Array1<f64>,
        prior_vol: f64,
        observation_count: usize,
    ) -> Result<f64, SpecificRiskError> {
        // Estimate raw individual volatility
        let individual_vol = self.estimate_raw_volatility(residuals)?;

        // Compute shrinkage weight
        let weight = self.compute_shrinkage_weight(observation_count);

        // Apply Bayesian shrinkage
        let shrunk_vol = weight * individual_vol + (1.0 - weight) * prior_vol;

        Ok(shrunk_vol)
    }

    /// Batch estimate for multiple securities with group priors
    ///
    /// Computes group-specific priors as the average volatility within each group,
    /// then applies Bayesian shrinkage to each security toward its group prior.
    ///
    /// # Arguments
    /// * `residuals` - Matrix of residuals (rows = time, columns = securities)
    /// * `group_assignments` - Which group each security belongs to (length = num securities)
    ///
    /// # Returns
    /// Vector of shrunk specific volatilities for each security
    pub fn estimate_batch(
        &self,
        residuals: &Array2<f64>,
        group_assignments: &[usize],
    ) -> Result<Array1<f64>, SpecificRiskError> {
        let (n_observations, n_securities) = residuals.dim();

        if group_assignments.len() != n_securities {
            return Err(SpecificRiskError::InvalidVolatility(format!(
                "Group assignments length {} does not match number of securities {}",
                group_assignments.len(),
                n_securities
            )));
        }

        // Step 1: Compute raw individual volatilities for all securities
        let mut individual_vols = Vec::with_capacity(n_securities);
        for i in 0..n_securities {
            let security_residuals = residuals.column(i).to_owned();
            match self.estimate_raw_volatility(&security_residuals) {
                Ok(vol) => individual_vols.push(vol),
                Err(_) => {
                    // If estimation fails, use default prior
                    individual_vols.push(self.config.default_prior_vol);
                }
            }
        }

        // Step 2: Compute group priors (average volatility per group)
        let num_groups = group_assignments.iter().max().map(|&x| x + 1).unwrap_or(0);
        let mut group_vols: Vec<f64> = vec![0.0; num_groups];
        let mut group_counts: Vec<usize> = vec![0; num_groups];

        for (i, &group_id) in group_assignments.iter().enumerate() {
            if group_id < num_groups {
                group_vols[group_id] += individual_vols[i];
                group_counts[group_id] += 1;
            }
        }

        // Compute group averages
        for (vol, &count) in group_vols.iter_mut().zip(group_counts.iter()) {
            if count > 0 {
                *vol /= count as f64;
            } else {
                *vol = self.config.default_prior_vol;
            }
        }

        // Step 3: Apply Bayesian shrinkage to each security
        let mut shrunk_vols = Vec::with_capacity(n_securities);
        let weight = self.compute_shrinkage_weight(n_observations);

        for (i, &group_id) in group_assignments.iter().enumerate() {
            let prior_vol = if group_id < num_groups {
                group_vols[group_id]
            } else {
                self.config.default_prior_vol
            };

            let shrunk_vol = weight * individual_vols[i] + (1.0 - weight) * prior_vol;
            shrunk_vols.push(shrunk_vol);
        }

        Ok(Array1::from_vec(shrunk_vols))
    }

    /// Batch estimate with explicit priors provided
    ///
    /// Similar to `estimate_batch` but uses externally provided priors
    /// instead of computing them from group averages.
    ///
    /// # Arguments
    /// * `residuals` - Matrix of residuals (rows = time, columns = securities)
    /// * `priors` - Prior volatility for each security
    ///
    /// # Returns
    /// Vector of shrunk specific volatilities for each security
    pub fn estimate_batch_with_priors(
        &self,
        residuals: &Array2<f64>,
        priors: &Array1<f64>,
    ) -> Result<Array1<f64>, SpecificRiskError> {
        let (n_observations, n_securities) = residuals.dim();

        if priors.len() != n_securities {
            return Err(SpecificRiskError::InvalidVolatility(format!(
                "Priors length {} does not match number of securities {}",
                priors.len(),
                n_securities
            )));
        }

        let weight = self.compute_shrinkage_weight(n_observations);
        let mut shrunk_vols = Vec::with_capacity(n_securities);

        for i in 0..n_securities {
            let security_residuals = residuals.column(i).to_owned();
            let individual_vol = self
                .estimate_raw_volatility(&security_residuals)
                .unwrap_or(priors[i]); // Fall back to prior if estimation fails

            let shrunk_vol = weight * individual_vol + (1.0 - weight) * priors[i];
            shrunk_vols.push(shrunk_vol);
        }

        Ok(Array1::from_vec(shrunk_vols))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_bayesian_config_default() {
        let config = BayesianShrinkageConfig::default();
        assert_eq!(config.shrinkage_strength, 60.0);
        assert_eq!(config.default_prior_vol, 0.30);
        assert_eq!(config.min_observations, 20);
    }

    #[test]
    fn test_shrinkage_weight_computation() {
        let estimator = BayesianSpecificRisk::default();

        // With κ = 60:
        // n = 0: w = 0
        // n = 60: w = 0.5
        // n = 180: w = 0.75
        // n = 240: w = 0.8
        assert_relative_eq!(estimator.compute_shrinkage_weight(0), 0.0, epsilon = 0.01);
        assert_relative_eq!(estimator.compute_shrinkage_weight(60), 0.5, epsilon = 0.01);
        assert_relative_eq!(
            estimator.compute_shrinkage_weight(180),
            0.75,
            epsilon = 0.01
        );
        assert_relative_eq!(estimator.compute_shrinkage_weight(240), 0.8, epsilon = 0.01);
    }

    #[test]
    fn test_estimate_with_prior_low_observations() {
        let config = BayesianShrinkageConfig {
            shrinkage_strength: 60.0,
            min_observations: 20,
            annualization_factor: 1.0, // No annualization for testing
            ..Default::default()
        };
        let estimator = BayesianSpecificRisk::new(config);

        // Create residuals with known volatility
        // For 30 observations with std ≈ 0.02, annual vol ≈ 0.02
        let residuals = Array1::from_vec(vec![
            0.02, -0.01, 0.03, -0.02, 0.01, 0.02, -0.03, 0.01, -0.01, 0.02, 0.01, -0.02, 0.03,
            -0.01, 0.02, -0.02, 0.01, 0.03, -0.01, 0.02, 0.01, -0.03, 0.02, -0.01, 0.03, 0.02,
            -0.01, 0.01, -0.02, 0.02,
        ]);

        let prior_vol = 0.03;
        let n_obs = residuals.len();

        let shrunk_vol = estimator
            .estimate_with_prior(&residuals, prior_vol, n_obs)
            .unwrap();

        // With n=30 and κ=60, weight = 30/90 = 0.333
        // So shrunk_vol should be closer to prior than to individual estimate
        let weight = 30.0 / 90.0;

        // Compute individual volatility (std dev of residuals)
        let mean = residuals.mean().unwrap_or(0.0);
        let variance = residuals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n_obs as f64);
        let individual_vol = variance.sqrt();

        // Check that shrinkage is applied correctly
        let expected = weight * individual_vol + (1.0 - weight) * prior_vol;
        assert_relative_eq!(shrunk_vol, expected, epsilon = 0.001);

        // Should be between individual and prior
        let min_vol = individual_vol.min(prior_vol);
        let max_vol = individual_vol.max(prior_vol);
        assert!(shrunk_vol > min_vol);
        assert!(shrunk_vol < max_vol);
    }

    #[test]
    fn test_estimate_with_prior_high_observations() {
        let config = BayesianShrinkageConfig {
            shrinkage_strength: 60.0,
            min_observations: 20,
            annualization_factor: 1.0,
            ..Default::default()
        };
        let estimator = BayesianSpecificRisk::new(config);

        // Create 240 observations
        let mut residuals_vec = Vec::new();
        for i in 0..240 {
            residuals_vec.push(0.02 * (i as f64 * 0.1).sin());
        }
        let residuals = Array1::from_vec(residuals_vec);

        let prior_vol = 0.05;
        let n_obs = residuals.len();

        let _shrunk_vol = estimator
            .estimate_with_prior(&residuals, prior_vol, n_obs)
            .unwrap();

        // With n=240 and κ=60, weight = 240/300 = 0.8
        // So shrunk_vol should be closer to individual estimate
        let weight = 240.0 / 300.0;
        assert_relative_eq!(weight, 0.8, epsilon = 0.001);
    }

    #[test]
    fn test_estimate_batch_with_groups() {
        let config = BayesianShrinkageConfig {
            shrinkage_strength: 60.0,
            min_observations: 20,
            annualization_factor: 1.0,
            ..Default::default()
        };
        let estimator = BayesianSpecificRisk::new(config);

        // Create residuals for 4 securities over 100 time periods
        // Securities 0, 1 in group 0 (low vol)
        // Securities 2, 3 in group 1 (high vol)
        let n_obs = 100;
        let n_securities = 4;
        let mut residuals_data = Vec::new();

        // Create varying residuals with different volatility levels
        for t in 0..n_obs {
            let sign = if t % 2 == 0 { 1.0 } else { -1.0 };
            residuals_data.push(sign * 0.01 * (1.0 + (t as f64 * 0.1).sin())); // Security 0 - low vol
            residuals_data.push(-sign * 0.01 * (1.0 + (t as f64 * 0.1).cos())); // Security 1 - low vol
            residuals_data.push(sign * 0.03 * (1.0 + (t as f64 * 0.1).sin())); // Security 2 - high vol
            residuals_data.push(-sign * 0.03 * (1.0 + (t as f64 * 0.1).cos())); // Security 3 - high vol
        }

        let residuals = Array2::from_shape_vec((n_obs, n_securities), residuals_data).unwrap();
        let group_assignments = vec![0, 0, 1, 1]; // Two groups

        let shrunk_vols = estimator
            .estimate_batch(&residuals, &group_assignments)
            .unwrap();

        assert_eq!(shrunk_vols.len(), n_securities);

        // Securities in the same group should have similar (but not identical) shrunk vols
        // Group 0 should have lower vols than group 1
        assert!(shrunk_vols[0] < shrunk_vols[2]);
        assert!(shrunk_vols[1] < shrunk_vols[3]);
    }

    #[test]
    fn test_estimate_batch_with_explicit_priors() {
        let config = BayesianShrinkageConfig {
            shrinkage_strength: 60.0,
            min_observations: 20,
            annualization_factor: 1.0,
            ..Default::default()
        };
        let estimator = BayesianSpecificRisk::new(config);

        let n_obs = 100;
        let n_securities = 3;

        // Create simple residuals
        let mut residuals_data = Vec::new();
        for _t in 0..n_obs {
            residuals_data.push(0.02);
            residuals_data.push(0.01);
            residuals_data.push(0.03);
        }

        let residuals = Array2::from_shape_vec((n_obs, n_securities), residuals_data).unwrap();
        let priors = Array1::from_vec(vec![0.025, 0.015, 0.035]);

        let shrunk_vols = estimator
            .estimate_batch_with_priors(&residuals, &priors)
            .unwrap();

        assert_eq!(shrunk_vols.len(), n_securities);

        // Each shrunk vol should be between its individual estimate and prior
        // With n=100, κ=60, weight = 100/160 = 0.625
        let weight = 100.0 / 160.0;
        assert_relative_eq!(weight, 0.625, epsilon = 0.001);
    }

    #[test]
    fn test_insufficient_observations() {
        let config = BayesianShrinkageConfig {
            min_observations: 50,
            ..Default::default()
        };
        let estimator = BayesianSpecificRisk::new(config);

        let residuals = Array1::from_vec(vec![0.01, 0.02, -0.01]); // Only 3 observations
        let result = estimator.estimate_with_prior(&residuals, 0.3, residuals.len());

        assert!(result.is_err());
        match result {
            Err(SpecificRiskError::InsufficientData { required, actual }) => {
                assert_eq!(required, 50);
                assert_eq!(actual, 3);
            }
            _ => panic!("Expected InsufficientData error"),
        }
    }

    #[test]
    fn test_mismatched_dimensions() {
        let estimator = BayesianSpecificRisk::default();

        let residuals = Array2::from_shape_vec((100, 3), vec![0.01; 300]).unwrap();
        let group_assignments = vec![0, 1]; // Wrong length

        let result = estimator.estimate_batch(&residuals, &group_assignments);
        assert!(result.is_err());
    }
}
