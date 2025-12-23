//! Size factors - measures of market capitalization
//!
//! Size factors capture the tendency of small-cap stocks to outperform large-cap
//! stocks (the size premium). Log market cap is the standard measure.

pub mod log_market_cap;

pub use log_market_cap::LogMarketCapFactor;
