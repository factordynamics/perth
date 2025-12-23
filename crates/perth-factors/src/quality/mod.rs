//! Quality factors - measures of fundamental business quality
//!
//! Quality factors capture the tendency of high-quality businesses (profitable,
//! stable, well-managed) to outperform. Common metrics include ROE, leverage,
//! earnings stability, and accruals.

pub mod composite;
pub mod leverage;
pub mod roe;

pub use composite::CompositeQualityFactor;
pub use leverage::LeverageFactor;
pub use roe::RoeFactor;
