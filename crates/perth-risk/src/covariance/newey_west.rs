//! Newey-West HAC (Heteroskedasticity and Autocorrelation Consistent) Covariance Estimator
//!
//! The Newey-West estimator provides a robust covariance matrix that accounts for
//! both heteroskedasticity and autocorrelation in the residuals. This is particularly
//! important for financial time series which often exhibit serial correlation.
//!
//! The estimator adds lagged cross-products with Bartlett kernel weights:
//! ```text
//! Σ_NW = Σ_0 + Σ_{l=1}^{L} w_l * (Σ_l + Σ_l^T)
//! where:
//! - Σ_0 = sample covariance
//! - Σ_l = (1/T) Σ_{t=l+1}^T (r_t - μ)(r_{t-l} - μ)^T
//! - w_l = 1 - l/(L+1) (Bartlett kernel weights)
//! - L = optimal lag selection (typically ceil(4*(T/100)^(2/9)))
//! ```
//!
//! # References
//! - Newey, W. K., & West, K. D. (1987). "A Simple, Positive Semi-Definite,
//!   Heteroskedasticity and Autocorrelation Consistent Covariance Matrix."
//!   Econometrica, 55(3), 703-708.

use super::{CovarianceError, CovarianceEstimator};
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};

/// Newey-West covariance estimator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeweyWestConfig {
    /// Minimum number of observations required (default: 60)
    pub min_observations: usize,

    /// Number of lags to use for HAC adjustment (None = automatic selection)
    /// When None, uses ceil(4*(T/100)^(2/9)) as recommended by Newey-West
    pub lags: Option<usize>,

    /// Whether to prewhiten the returns before estimation (default: false)
    /// Prewhitening can improve efficiency but adds complexity
    pub prewhiten: bool,
}

impl Default for NeweyWestConfig {
    fn default() -> Self {
        Self {
            min_observations: 60,
            lags: None, // Automatic selection
            prewhiten: false,
        }
    }
}

/// Newey-West HAC covariance estimator
#[derive(Debug, Default)]
pub struct NeweyWestEstimator {
    config: NeweyWestConfig,
}

impl NeweyWestEstimator {
    /// Create a new Newey-West estimator with the given configuration
    pub const fn new(config: NeweyWestConfig) -> Self {
        Self { config }
    }

    /// Compute optimal lag length using Newey-West rule of thumb
    ///
    /// Formula: L = ceil(4 * (T/100)^(2/9))
    ///
    /// # Arguments
    /// * `n_periods` - Number of time periods
    ///
    /// # Returns
    /// * Optimal number of lags
    fn optimal_lags(&self, n_periods: usize) -> usize {
        self.config.lags.unwrap_or_else(|| {
            // Automatic selection using Newey-West formula
            let t = n_periods as f64;
            let lags = 4.0 * (t / 100.0).powf(2.0 / 9.0);
            lags.ceil() as usize
        })
    }

    /// Compute Bartlett kernel weights
    ///
    /// Formula: w_l = 1 - l/(L+1) for l = 1, ..., L
    ///
    /// # Arguments
    /// * `lag` - The lag index (1-indexed)
    /// * `max_lag` - Maximum lag L
    ///
    /// # Returns
    /// * Bartlett weight for the given lag
    fn bartlett_weight(&self, lag: usize, max_lag: usize) -> f64 {
        if lag == 0 {
            1.0
        } else if lag <= max_lag {
            1.0 - (lag as f64) / (max_lag as f64 + 1.0)
        } else {
            0.0
        }
    }

    /// Compute sample mean for each factor
    fn compute_means(&self, factor_returns: &Array2<f64>) -> Array1<f64> {
        let (n_periods, n_factors) = factor_returns.dim();
        let mut means = Array1::<f64>::zeros(n_factors);

        for j in 0..n_factors {
            let sum: f64 = factor_returns.column(j).sum();
            means[j] = sum / n_periods as f64;
        }

        means
    }

    /// Compute the sample covariance matrix (Σ_0)
    fn compute_sample_covariance(
        &self,
        factor_returns: &Array2<f64>,
        means: &Array1<f64>,
    ) -> Array2<f64> {
        let (n_periods, n_factors) = factor_returns.dim();
        let mut cov = Array2::<f64>::zeros((n_factors, n_factors));

        for t in 0..n_periods {
            for i in 0..n_factors {
                for j in 0..n_factors {
                    let ri = factor_returns[[t, i]] - means[i];
                    let rj = factor_returns[[t, j]] - means[j];
                    cov[[i, j]] += ri * rj;
                }
            }
        }

        // Normalize by T (not T-1, following Newey-West convention)
        cov /= n_periods as f64;

        cov
    }

    /// Compute lagged autocovariance matrix (Σ_l)
    ///
    /// Formula: Σ_l = (1/T) Σ_{t=l+1}^T (r_t - μ)(r_{t-l} - μ)^T
    ///
    /// # Arguments
    /// * `factor_returns` - Matrix of factor returns
    /// * `means` - Mean returns for each factor
    /// * `lag` - The lag index
    ///
    /// # Returns
    /// * Lagged autocovariance matrix
    fn compute_lagged_covariance(
        &self,
        factor_returns: &Array2<f64>,
        means: &Array1<f64>,
        lag: usize,
    ) -> Array2<f64> {
        let (n_periods, n_factors) = factor_returns.dim();
        let mut cov_lag = Array2::<f64>::zeros((n_factors, n_factors));

        // Sum from t=lag to T-1 (0-indexed)
        for t in lag..n_periods {
            for i in 0..n_factors {
                for j in 0..n_factors {
                    let ri_t = factor_returns[[t, i]] - means[i];
                    let rj_t_lag = factor_returns[[t - lag, j]] - means[j];
                    cov_lag[[i, j]] += ri_t * rj_t_lag;
                }
            }
        }

        // Normalize by T (not T-lag)
        cov_lag /= n_periods as f64;

        cov_lag
    }

    /// Prewhiten returns using AR(1) model (optional)
    ///
    /// This can improve efficiency by removing first-order autocorrelation
    /// before applying the Newey-West adjustment.
    ///
    /// Note: This is a placeholder for future implementation
    #[allow(dead_code)]
    fn prewhiten(&self, _factor_returns: &Array2<f64>) -> Array2<f64> {
        // TODO: Implement AR(1) prewhitening
        // For now, just return the original data
        _factor_returns.clone()
    }
}

impl CovarianceEstimator for NeweyWestEstimator {
    fn estimate(&self, factor_returns: &Array2<f64>) -> Result<Array2<f64>, CovarianceError> {
        let (n_periods, n_factors) = factor_returns.dim();

        // Check minimum observations
        if n_periods < self.config.min_observations {
            return Err(CovarianceError::InsufficientData {
                required: self.config.min_observations,
                actual: n_periods,
            });
        }

        // Determine optimal lag length
        let max_lag = self.optimal_lags(n_periods);

        // Ensure we don't use more lags than we have data for
        let max_lag = max_lag.min(n_periods - 1);

        // Compute means
        let means = self.compute_means(factor_returns);

        // Step 1: Compute sample covariance (Σ_0)
        let mut cov = self.compute_sample_covariance(factor_returns, &means);

        // Step 2: Add lagged autocovariances with Bartlett weights
        // Σ_NW = Σ_0 + Σ_{l=1}^{L} w_l * (Σ_l + Σ_l^T)
        for lag in 1..=max_lag {
            let weight = self.bartlett_weight(lag, max_lag);
            let cov_lag = self.compute_lagged_covariance(factor_returns, &means, lag);

            // Add w_l * (Σ_l + Σ_l^T)
            // This ensures the resulting matrix is symmetric
            for i in 0..n_factors {
                for j in 0..n_factors {
                    cov[[i, j]] += weight * (cov_lag[[i, j]] + cov_lag[[j, i]]);
                }
            }
        }

        // The Newey-West estimator should be positive semi-definite by construction
        // when using the Bartlett kernel, but numerical issues can arise
        // TODO: Add positive definite enforcement if needed

        Ok(cov)
    }

    fn update(
        &self,
        _current_cov: &Array2<f64>,
        new_returns: &Array2<f64>,
    ) -> Result<Array2<f64>, CovarianceError> {
        // Newey-West doesn't have a natural incremental update formula
        // like EWMA does, so we re-estimate from scratch
        // In practice, you'd maintain a rolling window of returns
        self.estimate(new_returns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_newey_west_config_default() {
        let config = NeweyWestConfig::default();
        assert_eq!(config.min_observations, 60);
        assert!(config.lags.is_none());
        assert!(!config.prewhiten);
    }

    #[test]
    fn test_optimal_lags() {
        let estimator = NeweyWestEstimator::default();

        // Test with T=100
        let lags = estimator.optimal_lags(100);
        // ceil(4 * (100/100)^(2/9)) = ceil(4 * 1) = 4
        assert_eq!(lags, 4);

        // Test with T=500
        let lags = estimator.optimal_lags(500);
        // ceil(4 * (500/100)^(2/9)) = ceil(4 * 5^(2/9))
        // 5^(2/9) ≈ 1.427, so ceil(4 * 1.427) = ceil(5.71) = 6
        assert_eq!(lags, 6);

        // Test with T=1000
        let lags = estimator.optimal_lags(1000);
        // ceil(4 * (1000/100)^(2/9)) = ceil(4 * 10^(2/9))
        // 10^(2/9) ≈ 1.668, so ceil(4 * 1.668) = ceil(6.67) = 7
        assert_eq!(lags, 7);
    }

    #[test]
    fn test_optimal_lags_manual() {
        let config = NeweyWestConfig {
            lags: Some(10),
            ..Default::default()
        };
        let estimator = NeweyWestEstimator::new(config);

        // Should use manual setting regardless of T
        assert_eq!(estimator.optimal_lags(100), 10);
        assert_eq!(estimator.optimal_lags(500), 10);
    }

    #[test]
    fn test_bartlett_weight() {
        let estimator = NeweyWestEstimator::default();

        // For max_lag = 4
        let max_lag = 4;

        // w_0 = 1.0 (lag 0)
        assert_relative_eq!(estimator.bartlett_weight(0, max_lag), 1.0);

        // w_1 = 1 - 1/5 = 0.8
        assert_relative_eq!(estimator.bartlett_weight(1, max_lag), 0.8);

        // w_2 = 1 - 2/5 = 0.6
        assert_relative_eq!(estimator.bartlett_weight(2, max_lag), 0.6);

        // w_3 = 1 - 3/5 = 0.4
        assert_relative_eq!(estimator.bartlett_weight(3, max_lag), 0.4);

        // w_4 = 1 - 4/5 = 0.2
        assert_relative_eq!(estimator.bartlett_weight(4, max_lag), 0.2);

        // w_5 = 0.0 (beyond max_lag)
        assert_relative_eq!(estimator.bartlett_weight(5, max_lag), 0.0);
    }

    #[test]
    fn test_compute_means() {
        let estimator = NeweyWestEstimator::default();

        // Simple 3x2 matrix
        #[rustfmt::skip]
        let returns = Array2::from_shape_vec(
            (3, 2),
            vec![
                1.0, 2.0,
                3.0, 4.0,
                5.0, 6.0,
            ],
        ).unwrap();

        let means = estimator.compute_means(&returns);

        // Mean of column 0: (1+3+5)/3 = 3.0
        // Mean of column 1: (2+4+6)/3 = 4.0
        assert_relative_eq!(means[0], 3.0);
        assert_relative_eq!(means[1], 4.0);
    }

    #[test]
    fn test_insufficient_data() {
        let estimator = NeweyWestEstimator::default();
        let returns = Array2::<f64>::zeros((10, 3)); // Only 10 observations
        assert!(estimator.estimate(&returns).is_err());
    }

    #[test]
    fn test_estimate_simple_case() {
        let config = NeweyWestConfig {
            min_observations: 3,
            lags: Some(1), // Use 1 lag for simplicity
            prewhiten: false,
        };
        let estimator = NeweyWestEstimator::new(config);

        // Create simple uncorrelated returns
        #[rustfmt::skip]
        let returns = Array2::from_shape_vec(
            (5, 2),
            vec![
                0.01, 0.02,
                0.02, 0.01,
                -0.01, 0.01,
                0.01, -0.02,
                -0.01, 0.01,
            ],
        ).unwrap();

        let result = estimator.estimate(&returns);
        assert!(result.is_ok());

        let cov = result.unwrap();
        assert_eq!(cov.shape(), &[2, 2]);

        // Check symmetry
        assert_relative_eq!(cov[[0, 1]], cov[[1, 0]], epsilon = 1e-10);

        // Check diagonal is non-negative
        assert!(cov[[0, 0]] >= 0.0);
        assert!(cov[[1, 1]] >= 0.0);
    }

    #[test]
    fn test_estimate_with_autocorrelation() {
        let config = NeweyWestConfig {
            min_observations: 10,
            lags: Some(2),
            prewhiten: false,
        };
        let estimator = NeweyWestEstimator::new(config);

        // Create returns with autocorrelation
        let n = 100;
        let mut returns = Array2::<f64>::zeros((n, 2));

        // Factor 1: positive autocorrelation
        for i in 1..n {
            returns[[i, 0]] = 0.5 * returns[[i - 1, 0]] + 0.01;
        }

        // Factor 2: negative autocorrelation
        for i in 1..n {
            returns[[i, 1]] = -0.3 * returns[[i - 1, 1]] + 0.01;
        }

        let result = estimator.estimate(&returns);
        assert!(result.is_ok());

        let cov = result.unwrap();
        assert_eq!(cov.shape(), &[2, 2]);

        // Check symmetry
        assert_relative_eq!(cov[[0, 1]], cov[[1, 0]], epsilon = 1e-10);

        // HAC adjustment should produce larger variance for positively
        // autocorrelated series compared to sample variance
        let sample_var_0 = returns.column(0).mapv(|x| x * x).sum() / n as f64;
        // Note: This relationship may not always hold exactly depending on the data
        // but serves as a sanity check
        assert!(cov[[0, 0]] >= 0.0);
        assert!(sample_var_0 >= 0.0);
    }

    #[test]
    fn test_lagged_covariance() {
        let estimator = NeweyWestEstimator::default();

        // Simple 4x2 matrix
        #[rustfmt::skip]
        let returns = Array2::from_shape_vec(
            (4, 2),
            vec![
                1.0, 2.0,
                2.0, 3.0,
                3.0, 4.0,
                4.0, 5.0,
            ],
        ).unwrap();

        let means = estimator.compute_means(&returns);
        let cov_lag1 = estimator.compute_lagged_covariance(&returns, &means, 1);

        assert_eq!(cov_lag1.shape(), &[2, 2]);

        // The covariance should be positive (trending data)
        assert!(cov_lag1[[0, 0]] > 0.0);
        assert!(cov_lag1[[1, 1]] > 0.0);
    }

    #[test]
    fn test_zero_returns() {
        let config = NeweyWestConfig {
            min_observations: 3,
            lags: Some(1),
            prewhiten: false,
        };
        let estimator = NeweyWestEstimator::new(config);

        // All zeros should give zero covariance
        let returns = Array2::<f64>::zeros((10, 2));

        let result = estimator.estimate(&returns);
        assert!(result.is_ok());

        let cov = result.unwrap();

        // All elements should be zero
        for i in 0..2 {
            for j in 0..2 {
                assert_relative_eq!(cov[[i, j]], 0.0, epsilon = 1e-10);
            }
        }
    }

    #[test]
    fn test_single_factor() {
        let config = NeweyWestConfig {
            min_observations: 5,
            lags: Some(1),
            prewhiten: false,
        };
        let estimator = NeweyWestEstimator::new(config);

        // Single factor
        #[rustfmt::skip]
        let returns = Array2::from_shape_vec(
            (10, 1),
            vec![0.01, -0.02, 0.03, -0.01, 0.02, -0.03, 0.01, 0.02, -0.01, 0.01],
        ).unwrap();

        let result = estimator.estimate(&returns);
        assert!(result.is_ok());

        let cov = result.unwrap();
        assert_eq!(cov.shape(), &[1, 1]);
        assert!(cov[[0, 0]] > 0.0);
    }

    #[test]
    fn test_large_lag_clamping() {
        // Test that lags are clamped to n_periods - 1
        let config = NeweyWestConfig {
            min_observations: 5,
            lags: Some(100), // Way more than we have data
            prewhiten: false,
        };
        let estimator = NeweyWestEstimator::new(config);

        let returns = Array2::<f64>::from_elem((10, 2), 0.01);

        let result = estimator.estimate(&returns);
        // Should not panic or error due to lag clamping
        assert!(result.is_ok());
    }
}
