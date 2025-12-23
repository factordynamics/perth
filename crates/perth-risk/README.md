# perth-risk

Risk model for the Perth institutional-grade factor model.

## Features

- **Covariance Estimation**: Multiple methods for estimating factor covariance matrices
  - EWMA (Exponentially Weighted Moving Average)
  - Ledoit-Wolf shrinkage
  - Newey-West HAC (Heteroskedasticity and Autocorrelation Consistent)
  - Volatility regime detection
- **Specific Risk Estimation**: Idiosyncratic risk estimation with Bayesian shrinkage
- **Risk Model**: Complete multi-factor risk decomposition

## Architecture

The risk model decomposes portfolio variance into two components:

1. **Factor Risk**: `X * F * X^T` where F is the factor covariance matrix and X is the exposure matrix
2. **Specific Risk**: Diagonal matrix of idiosyncratic variances (security-specific risk)

### Modules

- `covariance`: Factor covariance estimation methods
  - `ewma`: Exponentially weighted moving average
  - `ledoit_wolf`: Ledoit-Wolf shrinkage estimator
  - `newey_west`: Newey-West HAC estimator
  - `regime`: Volatility regime detection
  - `utils`: Matrix utilities (positive definiteness, eigendecomposition)
- `specific_risk`: Idiosyncratic risk estimation
  - `estimate`: Core estimation logic
  - `bayesian`: Bayesian shrinkage methods
- `model`: Overall risk model combining factor and specific risk

## Types

- **CovarianceEstimator**: Trait for covariance estimation methods
- **EwmaCovarianceEstimator**: EWMA implementation
- **LedoitWolfEstimator**: Ledoit-Wolf shrinkage with configurable targets
- **NeweyWestEstimator**: HAC-consistent estimator
- **VolatilityRegimeDetector**: Regime detection for adaptive estimation
- **SpecificRiskEstimator**: Idiosyncratic risk estimation with shrinkage
- **RiskModel**: Complete risk model

## Usage

### EWMA Covariance Estimation

```rust
use perth_risk::covariance::{EwmaCovarianceEstimator, CovarianceEstimator};
use ndarray::Array2;

let estimator = EwmaCovarianceEstimator::try_default()?; // decay = 0.95

// factor_returns is T x N matrix (T observations, N factors)
let cov_matrix = estimator.estimate(&factor_returns)?;
```

### Ledoit-Wolf Shrinkage

```rust
use perth_risk::covariance::{LedoitWolfEstimator, LedoitWolfConfig, ShrinkageTarget};

let config = LedoitWolfConfig {
    target: ShrinkageTarget::ConstantCorrelation,
    min_shrinkage: 0.0,
    max_shrinkage: 1.0,
};

let estimator = LedoitWolfEstimator::new(config);
let cov_matrix = estimator.estimate(&factor_returns)?;
```

### Risk Model

```rust
use perth_risk::{RiskModel, EwmaCovarianceEstimator, SpecificRiskEstimator};

// Create covariance and specific risk estimators
let cov_estimator = EwmaCovarianceEstimator::try_default()?;
let spec_risk_estimator = SpecificRiskEstimator::default();

// Build risk model
let risk_model = RiskModel::default();

// Estimate risk from factor returns and residuals
let (factor_cov, specific_var) = risk_model.estimate(
    &factor_returns,
    &residuals,
)?;

// Compute portfolio risk
let portfolio_var = risk_model.portfolio_variance(
    &exposures,
    &holdings,
);
```

## Dependencies

- `toraniko-traits`: Common trait definitions
- `toraniko-math`: Mathematical utilities
- `ndarray`: N-dimensional array operations
- `polars`: DataFrame operations
- `chrono`: Date/time handling
- `serde`: Serialization support

## License

MIT
