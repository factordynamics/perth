//! Growth factors - measures of business growth
//!
//! Growth factors capture the tendency of high-growth companies to outperform.
//! Common metrics include earnings growth, revenue growth, and asset growth.

pub mod composite;
pub mod earnings_growth;
pub mod sales_growth;

pub use composite::CompositeGrowthFactor;
pub use earnings_growth::EarningsGrowthFactor;
pub use sales_growth::SalesGrowthFactor;
