#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/factordynamics/perth/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod growth;
pub mod liquidity;
pub mod momentum;
pub mod quality;
pub mod registry;
pub mod size;
pub mod value;
pub mod volatility;

// Re-export common types
pub use toraniko_traits::{Factor, FactorError, FactorKind, StyleFactor};

// Re-export registry types for convenience
pub use registry::{
    FactorCategory, FactorInfo, available_factors, factors_by_category, get_factor_info,
};
