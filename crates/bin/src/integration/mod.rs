//! Integration module for cross-sectional factor model.
//!
//! This module provides the data pipeline, factor computation, and sector encoding
//! needed to run proper factor attribution using toraniko-model's FactorReturnsEstimator.

pub(crate) mod cache_manager;
pub(crate) mod data_pipeline;
pub(crate) mod factor_engine;
pub(crate) mod sector_encoder;
