#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/factordynamics/perth/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod attribution;
pub mod export;
pub mod report;
pub mod summary;

pub use attribution::{FactorAttribution, PortfolioAttribution, SecurityAttribution};
pub use export::{
    ExportError, ExportFormat, Exporter, FactorExposureExport, PortfolioExport, PortfolioHolding,
    RiskDecompositionExport,
};
pub use report::{Report, ReportBuilder, ReportError};
pub use summary::{FactorRiskContribution, RiskSummary, generate_risk_summary};
