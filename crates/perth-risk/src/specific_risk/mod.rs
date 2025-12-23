//! Specific risk estimation
//!
//! Estimates the idiosyncratic (non-factor) risk for each security.
//! Specific risk is computed from the residuals after explaining returns
//! with factor exposures.

pub mod bayesian;
pub mod estimate;

pub use bayesian::{BayesianShrinkageConfig, BayesianSpecificRisk};
pub use estimate::SpecificRiskEstimator;

use thiserror::Error;

/// Errors that can occur during specific risk estimation
#[derive(Debug, Error)]
pub enum SpecificRiskError {
    /// Insufficient data for estimation
    #[error("Insufficient data: need at least {required} observations, got {actual}")]
    InsufficientData {
        /// Required number of observations
        required: usize,
        /// Actual number of observations
        actual: usize,
    },

    /// Invalid volatility estimate
    #[error("Invalid volatility: {0}")]
    InvalidVolatility(String),
}
