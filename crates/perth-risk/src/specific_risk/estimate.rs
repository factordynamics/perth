//! Specific risk estimator
//!
//! Estimates idiosyncratic volatility from factor model residuals.
//! Uses EWMA or other methods to estimate the variance of residual returns.

use super::SpecificRiskError;
use ndarray::Array1;
use serde::{Deserialize, Serialize};

/// Configuration for specific risk estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecificRiskConfig {
    /// Method to use for variance estimation
    pub method: VarianceMethod,

    /// EWMA decay factor (if using EWMA method)
    pub ewma_decay: f64,

    /// Minimum number of observations required
    pub min_observations: usize,

    /// Annualization factor (default: sqrt(252) for daily data)
    pub annualization_factor: f64,
}

/// Methods for estimating residual variance
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VarianceMethod {
    /// Simple historical standard deviation
    Historical,
    /// Exponentially weighted moving average
    Ewma,
}

impl Default for SpecificRiskConfig {
    fn default() -> Self {
        Self {
            method: VarianceMethod::Ewma,
            ewma_decay: 0.95,
            min_observations: 60,
            annualization_factor: (252.0_f64).sqrt(),
        }
    }
}

/// Specific risk estimator
#[derive(Debug, Default)]
pub struct SpecificRiskEstimator {
    config: SpecificRiskConfig,
}

impl SpecificRiskEstimator {
    /// Create a new specific risk estimator
    pub const fn new(config: SpecificRiskConfig) -> Self {
        Self { config }
    }

    /// Estimate specific risk from residual returns
    ///
    /// # Arguments
    /// * `residuals` - Residual returns after factor model explanation
    ///
    /// # Returns
    /// * Annualized specific volatility (standard deviation)
    pub fn estimate(&self, residuals: &Array1<f64>) -> Result<f64, SpecificRiskError> {
        let n = residuals.len();

        if n < self.config.min_observations {
            return Err(SpecificRiskError::InsufficientData {
                required: self.config.min_observations,
                actual: n,
            });
        }

        let variance = match self.config.method {
            VarianceMethod::Historical => self.historical_variance(residuals),
            VarianceMethod::Ewma => self.ewma_variance(residuals),
        };

        if variance < 0.0 {
            return Err(SpecificRiskError::InvalidVolatility(
                "Negative variance".to_string(),
            ));
        }

        // Return annualized volatility (standard deviation)
        Ok(variance.sqrt() * self.config.annualization_factor)
    }

    /// Compute historical (sample) variance
    fn historical_variance(&self, residuals: &Array1<f64>) -> f64 {
        let mean = residuals.mean().unwrap_or(0.0);
        let n = residuals.len() as f64;

        residuals.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)
    }

    /// Compute EWMA variance
    fn ewma_variance(&self, residuals: &Array1<f64>) -> f64 {
        if residuals.is_empty() {
            return 0.0;
        }

        let lambda = self.config.ewma_decay;
        let one_minus_lambda = 1.0 - lambda;

        // Initialize with first squared residual
        let mut variance = residuals[0].powi(2);

        // EWMA update: Var_t = λ * Var_{t-1} + (1-λ) * r_t^2
        for &residual in residuals.iter().skip(1) {
            variance = lambda * variance + one_minus_lambda * residual.powi(2);
        }

        variance
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_specific_risk_config_default() {
        let config = SpecificRiskConfig::default();
        assert_eq!(config.ewma_decay, 0.95);
        assert_eq!(config.min_observations, 60);
    }

    #[test]
    fn test_insufficient_data() {
        let estimator = SpecificRiskEstimator::default();
        let residuals = Array1::<f64>::zeros(10);
        assert!(estimator.estimate(&residuals).is_err());
    }

    #[test]
    fn test_historical_variance() {
        let estimator = SpecificRiskEstimator::new(SpecificRiskConfig {
            method: VarianceMethod::Historical,
            min_observations: 3,
            annualization_factor: 1.0, // No annualization for testing
            ..Default::default()
        });

        let residuals = Array1::from_vec(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let vol = estimator.estimate(&residuals).unwrap();

        // Sample variance of [1,2,3,4,5] = 2.5, std = sqrt(2.5) ≈ 1.58
        assert_relative_eq!(vol, 2.5_f64.sqrt(), epsilon = 0.01);
    }
}
