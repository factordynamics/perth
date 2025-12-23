#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/factordynamics/perth/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod covariance;
pub mod model;
pub mod specific_risk;

// Re-export main types
pub use covariance::{CovarianceEstimator, EwmaCovarianceEstimator};
pub use model::RiskModel;
pub use specific_risk::SpecificRiskEstimator;
