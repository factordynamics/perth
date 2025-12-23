//! Volatility factors - measures of risk and price variability
//!
//! Volatility factors capture systematic and idiosyncratic risk. Lower volatility
//! securities tend to outperform on a risk-adjusted basis (the low-volatility anomaly).

pub mod beta;
pub mod composite;
pub mod historical_vol;
pub mod idio_vol;

pub use beta::{BetaConfig, BetaFactor};
pub use composite::{CompositeVolatilityConfig, CompositeVolatilityFactor};
pub use historical_vol::{HistoricalVolatilityConfig, HistoricalVolatilityFactor};
pub use idio_vol::{IdioVolConfig, IdiosyncraticVolatilityFactor};
