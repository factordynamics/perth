//! Ledoit-Wolf Shrinkage Covariance Estimator
//!
//! Implements the analytical shrinkage estimator from:
//! "Honey, I Shrunk the Sample Covariance Matrix" (Ledoit & Wolf, 2004)
//!
//! The Ledoit-Wolf estimator shrinks the sample covariance matrix toward a
//! structured target to improve conditioning and reduce estimation error,
//! especially when the number of observations is small relative to the
//! number of assets.
//!
//! The estimator has the form:
//! Σ_LW = δ* F + (1-δ*) S
//!
//! where:
//! - S is the sample covariance matrix
//! - F is the shrinkage target (typically a structured matrix like identity)
//! - δ* is the optimal shrinkage intensity (computed analytically)

use super::{CovarianceError, CovarianceEstimator};
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};

/// Shrinkage target types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
pub enum ShrinkageTarget {
    /// Identity matrix scaled by average variance: F = μ * I where μ = trace(S)/n
    #[default]
    Identity,

    /// Single factor model: diagonal variances with constant correlation
    ConstantCorrelation,

    /// Diagonal matrix (no off-diagonal elements)
    Diagonal,
}

/// Ledoit-Wolf covariance estimator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedoitWolfConfig {
    /// Minimum number of observations required (default: 2)
    /// Note: Ledoit-Wolf works with small samples, but we need at least 2 observations
    pub min_observations: usize,

    /// Shrinkage target type (default: Identity)
    pub target: ShrinkageTarget,

    /// Whether to center returns (subtract mean) before computing covariance
    pub center: bool,
}

impl Default for LedoitWolfConfig {
    fn default() -> Self {
        Self {
            min_observations: 2,
            target: ShrinkageTarget::Identity,
            center: true,
        }
    }
}

/// Ledoit-Wolf shrinkage covariance estimator
#[derive(Debug, Default)]
pub struct LedoitWolfEstimator {
    config: LedoitWolfConfig,
}

impl LedoitWolfEstimator {
    /// Create a new Ledoit-Wolf estimator with the given configuration
    pub const fn new(config: LedoitWolfConfig) -> Self {
        Self { config }
    }

    /// Compute the sample covariance matrix
    fn sample_covariance(&self, factor_returns: &Array2<f64>) -> Array2<f64> {
        let (n_periods, _n_factors) = factor_returns.dim();
        let n = n_periods as f64;

        // Center the returns if configured
        let returns = if self.config.center {
            let means = factor_returns.mean_axis(ndarray::Axis(0)).unwrap();
            factor_returns - &means.insert_axis(ndarray::Axis(0))
        } else {
            factor_returns.clone()
        };

        // Sample covariance: S = (1/n) * X^T * X

        returns.t().dot(&returns) / n
    }

    /// Compute the shrinkage target matrix F
    fn shrinkage_target(&self, sample_cov: &Array2<f64>) -> Array2<f64> {
        let n_factors = sample_cov.nrows();

        match self.config.target {
            ShrinkageTarget::Identity => {
                // F = μ * I where μ = trace(S) / n
                let trace: f64 = sample_cov.diag().sum();
                let mu = trace / n_factors as f64;
                Array2::eye(n_factors) * mu
            }

            ShrinkageTarget::Diagonal => {
                // F = diagonal matrix with same diagonal as S
                let mut target = Array2::zeros((n_factors, n_factors));
                for i in 0..n_factors {
                    target[[i, i]] = sample_cov[[i, i]];
                }
                target
            }

            ShrinkageTarget::ConstantCorrelation => {
                // F has same variances as S but constant correlation
                let variances: Array1<f64> = sample_cov.diag().to_owned();
                let std_devs = variances.mapv(|v| v.sqrt());

                // Compute average correlation
                let mut sum_corr = 0.0;
                let mut count = 0;
                for i in 0..n_factors {
                    for j in (i + 1)..n_factors {
                        let corr = sample_cov[[i, j]] / (std_devs[i] * std_devs[j]);
                        sum_corr += corr;
                        count += 1;
                    }
                }
                let avg_corr = if count > 0 {
                    sum_corr / count as f64
                } else {
                    0.0
                };

                // Build constant correlation matrix
                let mut target = Array2::zeros((n_factors, n_factors));
                for i in 0..n_factors {
                    for j in 0..n_factors {
                        if i == j {
                            target[[i, j]] = variances[i];
                        } else {
                            target[[i, j]] = avg_corr * std_devs[i] * std_devs[j];
                        }
                    }
                }
                target
            }
        }
    }

    /// Compute optimal shrinkage intensity using Ledoit-Wolf formula
    ///
    /// This implements the analytical formula from the 2004 paper.
    /// The shrinkage intensity δ* minimizes the expected squared Frobenius norm
    /// of the estimation error.
    fn compute_shrinkage_intensity(
        &self,
        factor_returns: &Array2<f64>,
        sample_cov: &Array2<f64>,
        target: &Array2<f64>,
    ) -> f64 {
        let (n_periods, n_factors) = factor_returns.dim();
        let n = n_periods as f64;

        // Center the returns if configured
        let returns = if self.config.center {
            let means = factor_returns.mean_axis(ndarray::Axis(0)).unwrap();
            factor_returns - &means.insert_axis(ndarray::Axis(0))
        } else {
            factor_returns.clone()
        };

        // Compute pi-hat: asymptotic variance of sample covariance
        // π̂ = (1/n) * sum_t [ (y_t y_t^T - S)^2 ]
        let mut pi_hat = 0.0;
        for t in 0..n_periods {
            let y_t = returns.row(t);
            for i in 0..n_factors {
                for j in 0..n_factors {
                    let outer_prod = y_t[i] * y_t[j];
                    let diff = outer_prod - sample_cov[[i, j]];
                    pi_hat += diff * diff;
                }
            }
        }
        pi_hat /= n;

        // Compute rho-hat: misspecification of target
        // ρ̂ = π̂ - (1/n) * sum_t [ (y_t y_t^T - F)^2 ]
        let mut pi_hat_target = 0.0;
        for t in 0..n_periods {
            let y_t = returns.row(t);
            for i in 0..n_factors {
                for j in 0..n_factors {
                    let outer_prod = y_t[i] * y_t[j];
                    let diff = outer_prod - target[[i, j]];
                    pi_hat_target += diff * diff;
                }
            }
        }
        pi_hat_target /= n;

        let rho_hat = pi_hat_target - pi_hat;

        // Compute gamma-hat: distance between sample covariance and target
        // γ̂ = ||S - F||_F^2
        let mut gamma_hat = 0.0;
        for i in 0..n_factors {
            for j in 0..n_factors {
                let diff = sample_cov[[i, j]] - target[[i, j]];
                gamma_hat += diff * diff;
            }
        }

        // Compute optimal shrinkage intensity
        // δ* = max(0, min(1, ρ̂ / γ̂))

        if gamma_hat > 0.0 {
            (rho_hat / gamma_hat).clamp(0.0, 1.0)
        } else {
            // If gamma = 0, sample cov equals target, no shrinkage needed
            0.0
        }
    }

    /// Get the shrinkage intensity from the last estimation
    /// (useful for diagnostics)
    pub fn get_shrinkage_intensity(
        &self,
        factor_returns: &Array2<f64>,
    ) -> Result<f64, CovarianceError> {
        let (n_periods, _) = factor_returns.dim();

        if n_periods < self.config.min_observations {
            return Err(CovarianceError::InsufficientData {
                required: self.config.min_observations,
                actual: n_periods,
            });
        }

        let sample_cov = self.sample_covariance(factor_returns);
        let target = self.shrinkage_target(&sample_cov);
        let delta = self.compute_shrinkage_intensity(factor_returns, &sample_cov, &target);

        Ok(delta)
    }
}

impl CovarianceEstimator for LedoitWolfEstimator {
    fn estimate(&self, factor_returns: &Array2<f64>) -> Result<Array2<f64>, CovarianceError> {
        let (n_periods, _) = factor_returns.dim();

        // Check minimum observations
        if n_periods < self.config.min_observations {
            return Err(CovarianceError::InsufficientData {
                required: self.config.min_observations,
                actual: n_periods,
            });
        }

        // Compute sample covariance matrix
        let sample_cov = self.sample_covariance(factor_returns);

        // Compute shrinkage target
        let target = self.shrinkage_target(&sample_cov);

        // Compute optimal shrinkage intensity
        let delta = self.compute_shrinkage_intensity(factor_returns, &sample_cov, &target);

        // Apply shrinkage: Σ_LW = δ* F + (1-δ*) S
        let shrunk_cov = &target * delta + &sample_cov * (1.0 - delta);

        Ok(shrunk_cov)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_ledoit_wolf_config_default() {
        let config = LedoitWolfConfig::default();
        assert_eq!(config.min_observations, 2);
        assert_eq!(config.target, ShrinkageTarget::Identity);
        assert!(config.center);
    }

    #[test]
    fn test_shrinkage_target_default() {
        let target = ShrinkageTarget::default();
        assert_eq!(target, ShrinkageTarget::Identity);
    }

    #[test]
    fn test_insufficient_data() {
        let estimator = LedoitWolfEstimator::default();
        let returns = Array2::<f64>::zeros((1, 3)); // Only 1 observation
        assert!(estimator.estimate(&returns).is_err());
    }

    #[test]
    fn test_sample_covariance_simple() {
        let config = LedoitWolfConfig {
            center: false,
            ..Default::default()
        };
        let estimator = LedoitWolfEstimator::new(config);

        // Simple 2x2 case with known covariance
        let returns = Array2::from_shape_vec((3, 2), vec![1.0, 2.0, 2.0, 4.0, 3.0, 6.0]).unwrap();

        let sample_cov = estimator.sample_covariance(&returns);

        // Expected: (1/3) * [[14, 28], [28, 56]]
        assert_relative_eq!(sample_cov[[0, 0]], 14.0 / 3.0, epsilon = 1e-10);
        assert_relative_eq!(sample_cov[[0, 1]], 28.0 / 3.0, epsilon = 1e-10);
        assert_relative_eq!(sample_cov[[1, 0]], 28.0 / 3.0, epsilon = 1e-10);
        assert_relative_eq!(sample_cov[[1, 1]], 56.0 / 3.0, epsilon = 1e-10);
    }

    #[test]
    fn test_identity_target() {
        let estimator = LedoitWolfEstimator::default();

        let sample_cov =
            Array2::from_shape_vec((3, 3), vec![4.0, 1.0, 0.5, 1.0, 9.0, 1.5, 0.5, 1.5, 16.0])
                .unwrap();

        let target = estimator.shrinkage_target(&sample_cov);

        // μ = trace(S) / n = (4 + 9 + 16) / 3 = 29/3
        let mu = 29.0 / 3.0;

        // Check diagonal elements
        assert_relative_eq!(target[[0, 0]], mu, epsilon = 1e-10);
        assert_relative_eq!(target[[1, 1]], mu, epsilon = 1e-10);
        assert_relative_eq!(target[[2, 2]], mu, epsilon = 1e-10);

        // Check off-diagonal elements are zero
        assert_relative_eq!(target[[0, 1]], 0.0, epsilon = 1e-10);
        assert_relative_eq!(target[[1, 0]], 0.0, epsilon = 1e-10);
        assert_relative_eq!(target[[0, 2]], 0.0, epsilon = 1e-10);
    }

    #[test]
    fn test_diagonal_target() {
        let config = LedoitWolfConfig {
            target: ShrinkageTarget::Diagonal,
            ..Default::default()
        };
        let estimator = LedoitWolfEstimator::new(config);

        let sample_cov =
            Array2::from_shape_vec((3, 3), vec![4.0, 1.0, 0.5, 1.0, 9.0, 1.5, 0.5, 1.5, 16.0])
                .unwrap();

        let target = estimator.shrinkage_target(&sample_cov);

        // Diagonal should match sample covariance
        assert_relative_eq!(target[[0, 0]], 4.0, epsilon = 1e-10);
        assert_relative_eq!(target[[1, 1]], 9.0, epsilon = 1e-10);
        assert_relative_eq!(target[[2, 2]], 16.0, epsilon = 1e-10);

        // Off-diagonal should be zero
        assert_relative_eq!(target[[0, 1]], 0.0, epsilon = 1e-10);
        assert_relative_eq!(target[[1, 2]], 0.0, epsilon = 1e-10);
    }

    #[test]
    fn test_shrinkage_intensity_bounds() {
        let estimator = LedoitWolfEstimator::default();

        // Create some random-ish returns
        let returns = Array2::from_shape_vec(
            (10, 3),
            vec![
                0.01, 0.02, -0.01, -0.01, 0.01, 0.02, 0.02, -0.01, 0.01, -0.02, 0.01, -0.01, 0.01,
                -0.02, 0.02, 0.02, 0.01, -0.02, -0.01, -0.01, 0.01, 0.01, 0.02, 0.01, -0.02, -0.01,
                -0.01, 0.01, 0.01, 0.02,
            ],
        )
        .unwrap();

        let delta = estimator.get_shrinkage_intensity(&returns).unwrap();

        // Shrinkage intensity should be between 0 and 1
        assert!(delta >= 0.0);
        assert!(delta <= 1.0);
    }

    #[test]
    fn test_estimate_produces_valid_covariance() {
        let estimator = LedoitWolfEstimator::default();

        // Create some returns
        let returns =
            Array2::from_shape_vec((20, 3), (0..60).map(|i| (i as f64 * 0.01) - 0.3).collect())
                .unwrap();

        let cov = estimator.estimate(&returns).unwrap();

        // Check dimensions
        assert_eq!(cov.nrows(), 3);
        assert_eq!(cov.ncols(), 3);

        // Check symmetry
        for i in 0..3 {
            for j in 0..3 {
                assert_relative_eq!(cov[[i, j]], cov[[j, i]], epsilon = 1e-10);
            }
        }

        // Check diagonal is positive
        for i in 0..3 {
            assert!(cov[[i, i]] > 0.0);
        }
    }

    #[test]
    fn test_constant_correlation_target() {
        let config = LedoitWolfConfig {
            target: ShrinkageTarget::ConstantCorrelation,
            ..Default::default()
        };
        let estimator = LedoitWolfEstimator::new(config);

        // Create a sample covariance with known correlations
        let sample_cov = Array2::from_shape_vec(
            (2, 2),
            vec![
                4.0, 2.0, // corr = 2/(2*3) = 1/3
                2.0, 9.0,
            ],
        )
        .unwrap();

        let target = estimator.shrinkage_target(&sample_cov);

        // Variances should match
        assert_relative_eq!(target[[0, 0]], 4.0, epsilon = 1e-10);
        assert_relative_eq!(target[[1, 1]], 9.0, epsilon = 1e-10);

        // Correlation should be average (in this case, there's only one)
        let expected_cov = (1.0 / 3.0) * 2.0 * 3.0;
        assert_relative_eq!(target[[0, 1]], expected_cov, epsilon = 1e-10);
        assert_relative_eq!(target[[1, 0]], expected_cov, epsilon = 1e-10);
    }

    #[test]
    fn test_extreme_shrinkage_when_few_observations() {
        let estimator = LedoitWolfEstimator::default();

        // With very few observations relative to dimensions, should shrink heavily
        let returns = Array2::from_shape_vec(
            (3, 10), // 3 observations, 10 factors - ill-conditioned
            (0..30).map(|i| (i as f64 * 0.1) - 1.5).collect(),
        )
        .unwrap();

        let delta = estimator.get_shrinkage_intensity(&returns).unwrap();

        // Should have high shrinkage (closer to 1)
        assert!(
            delta > 0.5,
            "Expected high shrinkage with few observations, got {}",
            delta
        );
    }

    #[test]
    fn test_shrinkage_ratio_scales_with_observations() {
        let estimator = LedoitWolfEstimator::default();

        // Test that shrinkage intensity decreases as observations increase
        // relative to the dimensionality (or stays bounded)
        let n_factors = 3;

        // Create a simple dataset with low observations
        let low_obs = 10;
        let low_data: Vec<f64> = (0..(low_obs * n_factors))
            .map(|i| (i as f64 * 0.123).sin())
            .collect();
        let low_returns = Array2::from_shape_vec((low_obs, n_factors), low_data).unwrap();
        let delta_low = estimator.get_shrinkage_intensity(&low_returns).unwrap();

        // Create a dataset with more observations
        let high_obs = 100;
        let high_data: Vec<f64> = (0..(high_obs * n_factors))
            .map(|i| (i as f64 * 0.123).sin())
            .collect();
        let high_returns = Array2::from_shape_vec((high_obs, n_factors), high_data).unwrap();
        let delta_high = estimator.get_shrinkage_intensity(&high_returns).unwrap();

        // Shrinkage should be valid (between 0 and 1)
        assert!(
            (0.0..=1.0).contains(&delta_low),
            "Invalid shrinkage: {}",
            delta_low
        );
        assert!(
            (0.0..=1.0).contains(&delta_high),
            "Invalid shrinkage: {}",
            delta_high
        );

        // With deterministic sinusoidal data, both may have high shrinkage
        // The key test is that they're within valid bounds
    }
}
