//! Factor covariance estimation
//!
//! Provides methods for estimating the covariance matrix of factor returns,
//! which is a key component of multi-factor risk models.

pub mod ewma;
pub mod ledoit_wolf;
pub mod newey_west;
pub mod regime;
pub mod utils;

pub use ewma::EwmaCovarianceEstimator;
pub use ledoit_wolf::{LedoitWolfConfig, LedoitWolfEstimator, ShrinkageTarget};
pub use newey_west::{NeweyWestConfig, NeweyWestEstimator};
pub use regime::{VolatilityRegime, VolatilityRegimeConfig, VolatilityRegimeDetector};
pub use utils::{
    EigenDecomposition, PositiveDefiniteConfig, condition_number, enforce_positive_definite,
    is_positive_definite, is_positive_definite_with_tolerance, jacobi_eigendecomp,
    nearest_positive_definite,
};

use ndarray::Array2;
use thiserror::Error;

/// Errors that can occur during covariance estimation
#[derive(Debug, Error)]
pub enum CovarianceError {
    /// Insufficient data for estimation
    #[error("Insufficient data: need at least {required} observations, got {actual}")]
    InsufficientData {
        /// Required number of observations
        required: usize,
        /// Actual number of observations
        actual: usize,
    },

    /// Matrix is not positive definite
    #[error("Covariance matrix is not positive definite")]
    NotPositiveDefinite,

    /// Invalid decay parameter
    #[error("Invalid decay parameter: {0} (must be between 0 and 1)")]
    InvalidDecay(f64),

    /// Dimension mismatch
    #[error("Dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Expected dimension
        expected: usize,
        /// Actual dimension
        actual: usize,
    },

    /// Invalid parameter
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
}

/// Trait for covariance matrix estimators
pub trait CovarianceEstimator {
    /// Estimate the covariance matrix from factor returns
    ///
    /// # Arguments
    /// * `factor_returns` - Matrix where each row is a time period and each column is a factor
    ///
    /// # Returns
    /// * Estimated covariance matrix (N x N where N is number of factors)
    fn estimate(&self, factor_returns: &Array2<f64>) -> Result<Array2<f64>, CovarianceError>;

    /// Update an existing covariance estimate with new data
    ///
    /// Default implementation just re-estimates from scratch.
    fn update(
        &self,
        current_cov: &Array2<f64>,
        new_returns: &Array2<f64>,
    ) -> Result<Array2<f64>, CovarianceError> {
        // Default: just re-estimate (suboptimal but correct)
        let _ = current_cov; // Unused in default implementation
        self.estimate(new_returns)
    }
}
