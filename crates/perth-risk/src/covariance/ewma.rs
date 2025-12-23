//! Exponentially Weighted Moving Average (EWMA) Covariance Estimator
//!
//! EWMA gives more weight to recent observations, making it responsive to
//! changing market conditions. This is the industry-standard approach used
//! by risk models like MSCI Barra.
//!
//! The EWMA covariance between factors i and j is:
//! Cov_t(i,j) = λ * Cov_{t-1}(i,j) + (1-λ) * r_{i,t} * r_{j,t}
//!
//! where λ is the decay factor (typically 0.94 - 0.97 for daily data).

use super::{CovarianceError, CovarianceEstimator};
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};

/// EWMA covariance estimator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EwmaConfig {
    /// Decay factor λ (default: 0.95)
    /// Higher values = more weight on past, slower adaptation
    /// Lower values = more weight on recent, faster adaptation
    pub decay: f64,

    /// Minimum number of observations required (default: 60)
    pub min_observations: usize,

    /// Whether to adjust for small sample bias (default: true)
    pub bias_correction: bool,
}

impl Default for EwmaConfig {
    fn default() -> Self {
        Self {
            decay: 0.95,
            min_observations: 60,
            bias_correction: true,
        }
    }
}

/// EWMA covariance estimator
#[derive(Debug)]
pub struct EwmaCovarianceEstimator {
    config: EwmaConfig,
}

impl EwmaCovarianceEstimator {
    /// Create a new EWMA estimator with the given configuration
    pub fn new(config: EwmaConfig) -> Result<Self, CovarianceError> {
        if config.decay <= 0.0 || config.decay >= 1.0 {
            return Err(CovarianceError::InvalidDecay(config.decay));
        }
        Ok(Self { config })
    }

    /// Create with default configuration.
    ///
    /// # Errors
    /// Returns an error if the default configuration is invalid (should not happen).
    pub fn try_default() -> Result<Self, CovarianceError> {
        Self::new(EwmaConfig::default())
    }

    /// Get the half-life of the EWMA (in periods)
    ///
    /// Half-life = ln(0.5) / ln(λ)
    pub fn half_life(&self) -> f64 {
        0.5_f64.ln() / self.config.decay.ln()
    }

    /// Compute EWMA mean (for centering returns)
    fn ewma_mean(&self, returns: &Array1<f64>) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }

        let mut ewma = returns[0];
        let lambda = self.config.decay;

        for &ret in returns.iter().skip(1) {
            ewma = lambda * ewma + (1.0 - lambda) * ret;
        }

        ewma
    }
}

impl CovarianceEstimator for EwmaCovarianceEstimator {
    fn estimate(&self, factor_returns: &Array2<f64>) -> Result<Array2<f64>, CovarianceError> {
        let (n_periods, n_factors) = factor_returns.dim();

        // Check minimum observations
        if n_periods < self.config.min_observations {
            return Err(CovarianceError::InsufficientData {
                required: self.config.min_observations,
                actual: n_periods,
            });
        }

        // Initialize covariance matrix
        let mut cov = Array2::<f64>::zeros((n_factors, n_factors));

        // Compute EWMA means for each factor (optional centering)
        let means: Vec<f64> = (0..n_factors)
            .map(|i| self.ewma_mean(&factor_returns.column(i).to_owned()))
            .collect();

        // Initialize with first observation's outer product
        for i in 0..n_factors {
            for j in 0..n_factors {
                let ri = factor_returns[[0, i]] - means[i];
                let rj = factor_returns[[0, j]] - means[j];
                cov[[i, j]] = ri * rj;
            }
        }

        // EWMA update for subsequent observations
        let lambda = self.config.decay;
        let one_minus_lambda = 1.0 - lambda;

        for t in 1..n_periods {
            for i in 0..n_factors {
                for j in 0..n_factors {
                    let ri = factor_returns[[t, i]] - means[i];
                    let rj = factor_returns[[t, j]] - means[j];

                    // EWMA update: Cov_t = λ * Cov_{t-1} + (1-λ) * r_i * r_j
                    cov[[i, j]] = lambda * cov[[i, j]] + one_minus_lambda * ri * rj;
                }
            }
        }

        // Bias correction (similar to Pandas' adjust=True)
        if self.config.bias_correction {
            let weight_sum = (1.0 - lambda.powi(n_periods as i32)) / (1.0 - lambda);
            cov /= weight_sum / n_periods as f64;
        }

        // TODO: Add positive definite enforcement (eigenvalue clipping, etc.)
        // For now, we return as-is

        Ok(cov)
    }

    fn update(
        &self,
        current_cov: &Array2<f64>,
        new_returns: &Array2<f64>,
    ) -> Result<Array2<f64>, CovarianceError> {
        let (n_new, n_factors) = new_returns.dim();
        let (cov_n, cov_m) = current_cov.dim();

        // Validate dimensions
        if cov_n != n_factors || cov_m != n_factors {
            return Err(CovarianceError::DimensionMismatch {
                expected: n_factors,
                actual: cov_n,
            });
        }

        let mut cov = current_cov.clone();
        let lambda = self.config.decay;
        let one_minus_lambda = 1.0 - lambda;

        // Update with each new observation
        for t in 0..n_new {
            for i in 0..n_factors {
                for j in 0..n_factors {
                    let ri = new_returns[[t, i]];
                    let rj = new_returns[[t, j]];

                    // EWMA update
                    cov[[i, j]] = lambda * cov[[i, j]] + one_minus_lambda * ri * rj;
                }
            }
        }

        Ok(cov)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_ewma_config_default() {
        let config = EwmaConfig::default();
        assert_eq!(config.decay, 0.95);
        assert_eq!(config.min_observations, 60);
        assert!(config.bias_correction);
    }

    #[test]
    fn test_invalid_decay() {
        let config = EwmaConfig {
            decay: 1.5,
            ..Default::default()
        };
        assert!(EwmaCovarianceEstimator::new(config).is_err());
    }

    #[test]
    fn test_half_life() {
        let estimator = EwmaCovarianceEstimator::try_default().unwrap();
        let half_life = estimator.half_life();
        // For λ=0.95, half-life ≈ 13.5 periods
        assert_relative_eq!(half_life, 13.51, epsilon = 0.1);
    }

    #[test]
    fn test_insufficient_data() {
        let estimator = EwmaCovarianceEstimator::try_default().unwrap();
        let returns = Array2::<f64>::zeros((10, 3)); // Only 10 observations
        assert!(estimator.estimate(&returns).is_err());
    }

    // More comprehensive tests would go in integration tests
}
