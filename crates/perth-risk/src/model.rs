//! Risk Model
//!
//! Integrates factor covariance and specific risk into a complete multi-factor
//! risk model. Used for portfolio risk calculation and optimization.
//!
//! Portfolio variance decomposition:
//! Var(R_p) = w^T * (X * F * X^T + Δ) * w
//!
//! where:
//! - w = portfolio weights
//! - X = factor exposures matrix
//! - F = factor covariance matrix
//! - Δ = diagonal specific risk matrix

use crate::covariance::{CovarianceError, CovarianceEstimator};
use crate::specific_risk::{SpecificRiskError, SpecificRiskEstimator};
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Risk model errors
#[derive(Debug, Error)]
pub enum RiskModelError {
    /// Covariance estimation error
    #[error("Covariance error: {0}")]
    Covariance(#[from] CovarianceError),

    /// Specific risk estimation error
    #[error("Specific risk error: {0}")]
    SpecificRisk(#[from] SpecificRiskError),

    /// Dimension mismatch
    #[error("Dimension mismatch: {0}")]
    DimensionMismatch(String),

    /// Invalid portfolio weights
    #[error("Invalid portfolio weights: {0}")]
    InvalidWeights(String),
}

/// Risk model configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskModelConfig {
    /// Factor covariance estimation method
    pub covariance_method: String,

    /// Specific risk estimation method
    pub specific_risk_method: String,
}

impl Default for RiskModelConfig {
    fn default() -> Self {
        Self {
            covariance_method: "ewma".to_string(),
            specific_risk_method: "ewma".to_string(),
        }
    }
}

/// Multi-factor risk model
///
/// Combines factor covariance and specific risk estimates to compute
/// portfolio risk and risk decomposition.
#[derive(Debug)]
pub struct RiskModel {
    /// Factor covariance matrix (K x K)
    factor_covariance: Option<Array2<f64>>,
    /// Specific variances (N x 1)
    specific_variances: Option<Array1<f64>>,
}

impl Default for RiskModel {
    fn default() -> Self {
        Self::new()
    }
}

impl RiskModel {
    /// Create a new risk model
    pub const fn new() -> Self {
        Self {
            factor_covariance: None,
            specific_variances: None,
        }
    }

    /// Fit the risk model to factor returns and residuals
    ///
    /// # Arguments
    /// * `factor_returns` - Historical factor returns (T x K)
    /// * `residuals` - Residuals for each security (T x N)
    /// * `covariance_estimator` - Estimator for factor covariance
    /// * `specific_risk_estimator` - Estimator for specific risk
    pub fn fit<C>(
        &mut self,
        factor_returns: &Array2<f64>,
        residuals: &Array2<f64>,
        covariance_estimator: &C,
        specific_risk_estimator: &SpecificRiskEstimator,
    ) -> Result<(), RiskModelError>
    where
        C: CovarianceEstimator,
    {
        // Estimate factor covariance matrix
        self.factor_covariance = Some(covariance_estimator.estimate(factor_returns)?);

        // Estimate specific risk for each security
        let n_securities = residuals.ncols();
        let mut specific_vars = Array1::<f64>::zeros(n_securities);

        for i in 0..n_securities {
            let residual_series = residuals.column(i).to_owned();
            let specific_vol = specific_risk_estimator.estimate(&residual_series)?;
            specific_vars[i] = specific_vol.powi(2); // Store as variance
        }

        self.specific_variances = Some(specific_vars);

        Ok(())
    }

    /// Compute portfolio variance
    ///
    /// # Arguments
    /// * `weights` - Portfolio weights (N x 1)
    /// * `exposures` - Factor exposures for each security (N x K)
    ///
    /// # Returns
    /// * Total portfolio variance
    pub fn portfolio_variance(
        &self,
        weights: &Array1<f64>,
        exposures: &Array2<f64>,
    ) -> Result<f64, RiskModelError> {
        let factor_cov = self
            .factor_covariance
            .as_ref()
            .ok_or_else(|| RiskModelError::DimensionMismatch("Model not fitted".to_string()))?;

        let specific_vars = self
            .specific_variances
            .as_ref()
            .ok_or_else(|| RiskModelError::DimensionMismatch("Model not fitted".to_string()))?;

        // Validate dimensions
        let n_securities = weights.len();
        let (n_exp, _n_factors) = exposures.dim();

        if n_exp != n_securities {
            return Err(RiskModelError::DimensionMismatch(format!(
                "Exposures ({}) don't match weights ({})",
                n_exp, n_securities
            )));
        }

        if specific_vars.len() != n_securities {
            return Err(RiskModelError::DimensionMismatch(format!(
                "Specific vars ({}) don't match weights ({})",
                specific_vars.len(),
                n_securities
            )));
        }

        // Factor risk: w^T * X * F * X^T * w
        // = (X^T * w)^T * F * (X^T * w)
        let factor_weights = exposures.t().dot(weights); // K x 1
        let factor_var = factor_weights.dot(&factor_cov.dot(&factor_weights));

        // Specific risk: w^T * Δ * w = sum(w_i^2 * σ_i^2)
        let specific_var = weights
            .iter()
            .zip(specific_vars.iter())
            .map(|(w, var)| w.powi(2) * var)
            .sum::<f64>();

        Ok(factor_var + specific_var)
    }

    /// Compute portfolio volatility (standard deviation)
    pub fn portfolio_volatility(
        &self,
        weights: &Array1<f64>,
        exposures: &Array2<f64>,
    ) -> Result<f64, RiskModelError> {
        Ok(self.portfolio_variance(weights, exposures)?.sqrt())
    }

    /// Decompose portfolio risk into factor and specific components
    ///
    /// # Returns
    /// * (factor_risk, specific_risk, total_risk)
    pub fn risk_decomposition(
        &self,
        weights: &Array1<f64>,
        exposures: &Array2<f64>,
    ) -> Result<(f64, f64, f64), RiskModelError> {
        let factor_cov = self
            .factor_covariance
            .as_ref()
            .ok_or_else(|| RiskModelError::DimensionMismatch("Model not fitted".to_string()))?;

        let specific_vars = self
            .specific_variances
            .as_ref()
            .ok_or_else(|| RiskModelError::DimensionMismatch("Model not fitted".to_string()))?;

        // Factor risk
        let factor_weights = exposures.t().dot(weights);
        let factor_var = factor_weights.dot(&factor_cov.dot(&factor_weights));
        let factor_risk = factor_var.sqrt();

        // Specific risk
        let specific_var = weights
            .iter()
            .zip(specific_vars.iter())
            .map(|(w, var)| w.powi(2) * var)
            .sum::<f64>();
        let specific_risk = specific_var.sqrt();

        // Total risk
        let total_risk = (factor_var + specific_var).sqrt();

        Ok((factor_risk, specific_risk, total_risk))
    }

    /// Get the factor covariance matrix
    pub const fn factor_covariance(&self) -> Option<&Array2<f64>> {
        self.factor_covariance.as_ref()
    }

    /// Get the specific variances
    pub const fn specific_variances(&self) -> Option<&Array1<f64>> {
        self.specific_variances.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_model_creation() {
        let model = RiskModel::default();
        assert!(model.factor_covariance.is_none());
        assert!(model.specific_variances.is_none());
    }

    // More comprehensive tests would require setting up full factor returns
    // and residuals - see integration tests
}
