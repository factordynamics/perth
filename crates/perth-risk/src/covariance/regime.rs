//! Volatility Regime Detection and Covariance Scaling
//!
//! This module provides functionality to detect volatility regimes (low, normal, high)
//! and scale covariance matrices accordingly. This is crucial for risk models to adapt
//! to changing market conditions.
//!
//! The approach:
//! 1. Compute realized volatility over a short-term window (e.g., 21 days)
//! 2. Compare to long-term volatility (e.g., 252 days)
//! 3. Classify regime based on the ratio
//! 4. Scale covariance matrices to reflect current regime

use super::CovarianceError;
use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};

/// Configuration for volatility regime detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolatilityRegimeConfig {
    /// Short-term window for current volatility (default: 21 days)
    /// This represents approximately one trading month
    pub short_window: usize,

    /// Long-term window for historical volatility (default: 252 days)
    /// This represents approximately one trading year
    pub long_window: usize,

    /// Low volatility threshold as ratio to long-term (default: 0.75)
    /// If short_vol / long_vol < low_vol_threshold, regime is Low
    pub low_vol_threshold: f64,

    /// High volatility threshold as ratio to long-term (default: 1.5)
    /// If short_vol / long_vol > high_vol_threshold, regime is High
    pub high_vol_threshold: f64,

    /// Maximum scaling factor (default: 3.0)
    /// Prevents excessive scaling in extreme conditions
    pub max_scale: f64,
}

impl Default for VolatilityRegimeConfig {
    fn default() -> Self {
        Self {
            short_window: 21,
            long_window: 252,
            low_vol_threshold: 0.75,
            high_vol_threshold: 1.5,
            max_scale: 3.0,
        }
    }
}

/// Volatility regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolatilityRegime {
    /// Low volatility regime (calm markets)
    Low,
    /// Normal volatility regime
    Normal,
    /// High volatility regime (stressed markets)
    High,
}

/// Volatility regime detector
///
/// This detector compares short-term realized volatility to long-term volatility
/// to classify the current market regime and compute appropriate scaling factors
/// for covariance matrices.
#[derive(Debug, Clone)]
pub struct VolatilityRegimeDetector {
    config: VolatilityRegimeConfig,
}

impl VolatilityRegimeDetector {
    /// Create a new volatility regime detector with the given configuration
    pub fn new(config: VolatilityRegimeConfig) -> Result<Self, CovarianceError> {
        // Validate configuration
        if config.short_window == 0 {
            return Err(CovarianceError::InsufficientData {
                required: 1,
                actual: 0,
            });
        }
        if config.long_window <= config.short_window {
            return Err(CovarianceError::InvalidParameter(
                "long_window must be greater than short_window".to_string(),
            ));
        }
        if config.low_vol_threshold >= config.high_vol_threshold {
            return Err(CovarianceError::InvalidParameter(
                "low_vol_threshold must be less than high_vol_threshold".to_string(),
            ));
        }
        if config.max_scale <= 0.0 {
            return Err(CovarianceError::InvalidParameter(
                "max_scale must be positive".to_string(),
            ));
        }

        Ok(Self { config })
    }

    /// Create a detector with default configuration.
    ///
    /// # Errors
    /// Returns an error if the default configuration is invalid (should not happen).
    pub fn try_default() -> Result<Self, CovarianceError> {
        Self::new(VolatilityRegimeConfig::default())
    }

    /// Compute realized volatility for a window of returns
    ///
    /// Uses the standard deviation of returns (not annualized)
    fn realized_volatility(&self, returns: &Array1<f64>) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }

        // Compute mean
        let mean = returns.mean().unwrap_or(0.0);

        // Compute variance (using n-1 degrees of freedom for sample variance)
        let n = returns.len() as f64;
        if n <= 1.0 {
            return 0.0;
        }

        let variance = returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);

        variance.sqrt()
    }

    /// Detect the current volatility regime
    ///
    /// # Arguments
    /// * `returns` - Array of returns (most recent last)
    ///
    /// # Returns
    /// * The detected volatility regime
    ///
    /// # Panics
    /// * If returns has fewer observations than long_window
    pub fn detect_regime(&self, returns: &Array1<f64>) -> VolatilityRegime {
        let n = returns.len();

        // Ensure we have enough data
        assert!(
            n >= self.config.long_window,
            "Need at least {} observations, got {}",
            self.config.long_window,
            n
        );

        // Compute short-term volatility (most recent observations)
        let short_start = n.saturating_sub(self.config.short_window);
        let short_returns = returns.slice(ndarray::s![short_start..]).to_owned();
        let short_vol = self.realized_volatility(&short_returns);

        // Compute long-term volatility
        let long_start = n.saturating_sub(self.config.long_window);
        let long_returns = returns.slice(ndarray::s![long_start..]).to_owned();
        let long_vol = self.realized_volatility(&long_returns);

        // Avoid division by zero
        if long_vol == 0.0 {
            return VolatilityRegime::Normal;
        }

        // Compute volatility ratio
        let vol_ratio = short_vol / long_vol;

        // Classify regime
        if vol_ratio < self.config.low_vol_threshold {
            VolatilityRegime::Low
        } else if vol_ratio > self.config.high_vol_threshold {
            VolatilityRegime::High
        } else {
            VolatilityRegime::Normal
        }
    }

    /// Compute the scaling factor based on current vs historical volatility
    ///
    /// The scaling factor is the ratio of short-term to long-term volatility,
    /// capped at max_scale to prevent excessive adjustments.
    ///
    /// # Arguments
    /// * `returns` - Array of returns (most recent last)
    ///
    /// # Returns
    /// * Scaling factor to apply to covariance matrix
    ///
    /// # Panics
    /// * If returns has fewer observations than long_window
    pub fn compute_scale_factor(&self, returns: &Array1<f64>) -> f64 {
        let n = returns.len();

        // Ensure we have enough data
        assert!(
            n >= self.config.long_window,
            "Need at least {} observations, got {}",
            self.config.long_window,
            n
        );

        // Compute short-term volatility
        let short_start = n.saturating_sub(self.config.short_window);
        let short_returns = returns.slice(ndarray::s![short_start..]).to_owned();
        let short_vol = self.realized_volatility(&short_returns);

        // Compute long-term volatility
        let long_start = n.saturating_sub(self.config.long_window);
        let long_returns = returns.slice(ndarray::s![long_start..]).to_owned();
        let long_vol = self.realized_volatility(&long_returns);

        // Avoid division by zero - return 1.0 (no scaling) if long_vol is zero
        if long_vol == 0.0 {
            return 1.0;
        }

        // Compute vol ratio (this is the variance scale factor squared)
        let vol_ratio = short_vol / long_vol;

        // For covariance, we need variance scaling, which is vol_ratio^2
        // But we cap it at max_scale for both upper and lower bounds
        let variance_scale = vol_ratio.powi(2);

        // Cap the scaling factor
        variance_scale
            .max(1.0 / self.config.max_scale)
            .min(self.config.max_scale)
    }

    /// Scale a covariance matrix for the current regime
    ///
    /// This multiplies the entire covariance matrix by the variance scaling factor,
    /// which is appropriate since Cov(aX, aY) = a^2 * Cov(X, Y).
    ///
    /// # Arguments
    /// * `cov` - The covariance matrix to scale
    /// * `returns` - Array of returns used to detect regime
    ///
    /// # Returns
    /// * Scaled covariance matrix
    ///
    /// # Panics
    /// * If returns has fewer observations than long_window
    pub fn scale_covariance(&self, cov: &Array2<f64>, returns: &Array1<f64>) -> Array2<f64> {
        let scale_factor = self.compute_scale_factor(returns);
        cov * scale_factor
    }

    /// Get the current configuration
    pub const fn config(&self) -> &VolatilityRegimeConfig {
        &self.config
    }

    /// Compute both the regime and scale factor
    ///
    /// This is a convenience method that computes both values in one pass
    /// to avoid redundant calculations.
    ///
    /// # Arguments
    /// * `returns` - Array of returns (most recent last)
    ///
    /// # Returns
    /// * Tuple of (regime, scale_factor)
    ///
    /// # Panics
    /// * If returns has fewer observations than long_window
    pub fn analyze(&self, returns: &Array1<f64>) -> (VolatilityRegime, f64) {
        let regime = self.detect_regime(returns);
        let scale_factor = self.compute_scale_factor(returns);
        (regime, scale_factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    fn create_constant_returns(value: f64, len: usize) -> Array1<f64> {
        Array1::from_elem(len, value)
    }

    fn create_varying_returns(base_vol: f64, len: usize) -> Array1<f64> {
        // Create returns with specified volatility (approximate)
        let mut returns = Array1::<f64>::zeros(len);
        for i in 0..len {
            let phase = 2.0 * std::f64::consts::PI * i as f64 / 20.0;
            returns[i] = base_vol * phase.sin();
        }
        returns
    }

    #[test]
    fn test_config_default() {
        let config = VolatilityRegimeConfig::default();
        assert_eq!(config.short_window, 21);
        assert_eq!(config.long_window, 252);
        assert_eq!(config.low_vol_threshold, 0.75);
        assert_eq!(config.high_vol_threshold, 1.5);
        assert_eq!(config.max_scale, 3.0);
    }

    #[test]
    fn test_invalid_config_short_window() {
        let config = VolatilityRegimeConfig {
            short_window: 0,
            ..Default::default()
        };
        assert!(VolatilityRegimeDetector::new(config).is_err());
    }

    #[test]
    fn test_invalid_config_window_order() {
        let config = VolatilityRegimeConfig {
            short_window: 100,
            long_window: 50,
            ..Default::default()
        };
        assert!(VolatilityRegimeDetector::new(config).is_err());
    }

    #[test]
    fn test_invalid_config_thresholds() {
        let config = VolatilityRegimeConfig {
            low_vol_threshold: 2.0,
            high_vol_threshold: 1.0,
            ..Default::default()
        };
        assert!(VolatilityRegimeDetector::new(config).is_err());
    }

    #[test]
    fn test_invalid_config_max_scale() {
        let config = VolatilityRegimeConfig {
            max_scale: -1.0,
            ..Default::default()
        };
        assert!(VolatilityRegimeDetector::new(config).is_err());
    }

    #[test]
    fn test_realized_volatility_constant() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();
        let returns = create_constant_returns(0.01, 100);
        let vol = detector.realized_volatility(&returns);
        // Constant returns should have zero volatility
        assert_relative_eq!(vol, 0.0, epsilon = 1e-10);
    }

    #[test]
    fn test_realized_volatility_varying() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();
        // Create returns with mean 0 and known std dev
        let returns = Array1::from_vec(vec![0.01, -0.01, 0.02, -0.02, 0.0]);
        let vol = detector.realized_volatility(&returns);
        // Should be positive
        assert!(vol > 0.0);
    }

    #[test]
    fn test_detect_regime_normal() {
        let config = VolatilityRegimeConfig {
            short_window: 20,
            long_window: 100,
            low_vol_threshold: 0.75,
            high_vol_threshold: 1.5,
            max_scale: 3.0,
        };
        let detector = VolatilityRegimeDetector::new(config).unwrap();

        // Create returns where short and long vol are similar
        let returns = create_varying_returns(0.01, 100);
        let regime = detector.detect_regime(&returns);

        assert_eq!(regime, VolatilityRegime::Normal);
    }

    #[test]
    fn test_detect_regime_high() {
        let config = VolatilityRegimeConfig {
            short_window: 20,
            long_window: 100,
            low_vol_threshold: 0.75,
            high_vol_threshold: 1.5,
            max_scale: 3.0,
        };
        let detector = VolatilityRegimeDetector::new(config).unwrap();

        // Create returns where recent vol is much higher
        let mut returns = create_varying_returns(0.01, 80);
        let high_vol_returns = create_varying_returns(0.05, 20);

        // Append high volatility returns
        returns
            .append(ndarray::Axis(0), high_vol_returns.view())
            .unwrap();

        let regime = detector.detect_regime(&returns);
        assert_eq!(regime, VolatilityRegime::High);
    }

    #[test]
    fn test_detect_regime_low() {
        let config = VolatilityRegimeConfig {
            short_window: 20,
            long_window: 100,
            low_vol_threshold: 0.75,
            high_vol_threshold: 1.5,
            max_scale: 3.0,
        };
        let detector = VolatilityRegimeDetector::new(config).unwrap();

        // Create returns where recent vol is much lower
        let mut returns = create_varying_returns(0.05, 80);
        let low_vol_returns = create_varying_returns(0.005, 20);

        // Append low volatility returns
        returns
            .append(ndarray::Axis(0), low_vol_returns.view())
            .unwrap();

        let regime = detector.detect_regime(&returns);
        assert_eq!(regime, VolatilityRegime::Low);
    }

    #[test]
    fn test_compute_scale_factor_normal() {
        let config = VolatilityRegimeConfig {
            short_window: 20,
            long_window: 100,
            max_scale: 3.0,
            ..Default::default()
        };
        let detector = VolatilityRegimeDetector::new(config).unwrap();

        // Similar volatility should give scale factor close to 1
        let returns = create_varying_returns(0.01, 100);
        let scale = detector.compute_scale_factor(&returns);

        // Should be close to 1.0
        assert_relative_eq!(scale, 1.0, epsilon = 0.2);
    }

    #[test]
    fn test_compute_scale_factor_capped() {
        let max_scale = 2.0;
        let config = VolatilityRegimeConfig {
            short_window: 20,
            long_window: 100,
            max_scale,
            ..Default::default()
        };
        let detector = VolatilityRegimeDetector::new(config).unwrap();

        // Create extreme volatility difference
        let mut returns = create_varying_returns(0.001, 80);
        let high_vol_returns = create_varying_returns(0.1, 20);
        returns
            .append(ndarray::Axis(0), high_vol_returns.view())
            .unwrap();

        let scale = detector.compute_scale_factor(&returns);

        // Should be capped at max_scale
        assert!(scale <= max_scale);
    }

    #[test]
    fn test_compute_scale_factor_floor() {
        let max_scale = 3.0;
        let config = VolatilityRegimeConfig {
            short_window: 20,
            long_window: 100,
            max_scale,
            ..Default::default()
        };
        let detector = VolatilityRegimeDetector::new(config).unwrap();

        // Create extreme low volatility
        let mut returns = create_varying_returns(0.1, 80);
        let low_vol_returns = create_varying_returns(0.001, 20);
        returns
            .append(ndarray::Axis(0), low_vol_returns.view())
            .unwrap();

        let scale = detector.compute_scale_factor(&returns);

        // Should be floored at 1/max_scale
        assert!(scale >= 1.0 / max_scale);
    }

    #[test]
    fn test_scale_covariance() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();

        // Create a simple 2x2 covariance matrix
        let cov = Array2::from_shape_vec((2, 2), vec![1.0, 0.5, 0.5, 1.0]).unwrap();

        // Create returns that should give some scaling
        let mut returns = create_varying_returns(0.01, 232);
        let high_vol_returns = create_varying_returns(0.02, 20);
        returns
            .append(ndarray::Axis(0), high_vol_returns.view())
            .unwrap();

        let scaled_cov = detector.scale_covariance(&cov, &returns);

        // Scaled covariance should be larger (high vol regime)
        assert!(scaled_cov[[0, 0]] >= cov[[0, 0]]);
        assert!(scaled_cov[[1, 1]] >= cov[[1, 1]]);

        // Should maintain symmetry
        assert_relative_eq!(scaled_cov[[0, 1]], scaled_cov[[1, 0]], epsilon = 1e-10);
    }

    #[test]
    fn test_analyze() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();

        // Create returns for analysis
        let returns = create_varying_returns(0.01, 300);

        let (regime, scale_factor) = detector.analyze(&returns);

        // Should detect a regime
        assert!(matches!(
            regime,
            VolatilityRegime::Low | VolatilityRegime::Normal | VolatilityRegime::High
        ));

        // Scale factor should be positive
        assert!(scale_factor > 0.0);
    }

    #[test]
    fn test_zero_volatility_returns() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();

        // All zero returns (edge case)
        let returns = Array1::<f64>::zeros(252);

        // Should default to normal regime and scale factor of 1.0
        let regime = detector.detect_regime(&returns);
        let scale = detector.compute_scale_factor(&returns);

        assert_eq!(regime, VolatilityRegime::Normal);
        assert_relative_eq!(scale, 1.0, epsilon = 1e-10);
    }

    #[test]
    #[should_panic]
    fn test_insufficient_data_detect_regime() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();
        let returns = Array1::<f64>::zeros(10); // Not enough data
        let _ = detector.detect_regime(&returns);
    }

    #[test]
    #[should_panic]
    fn test_insufficient_data_compute_scale() {
        let detector = VolatilityRegimeDetector::try_default().unwrap();
        let returns = Array1::<f64>::zeros(10); // Not enough data
        let _ = detector.compute_scale_factor(&returns);
    }
}
