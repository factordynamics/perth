#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/factordynamics/perth/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod universe;

// Re-export main types from sub-crates
pub use perth_data as data;
pub use perth_factors as factors;
pub use perth_output as output;
pub use perth_risk as risk;

// Re-export common universe types
pub use universe::{Universe, gics::GicsSector, sp500::SP500Universe};

/// Version information.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
