//! Value factors - measures of relative cheapness
//!
//! Value factors capture the tendency of undervalued securities to outperform.
//! Common value metrics include book-to-price, earnings yield, and sales-to-price ratios.

pub mod book_to_price;
pub mod composite;
pub mod earnings_yield;

pub use book_to_price::BookToPriceFactor;
pub use composite::CompositeValueFactor;
pub use earnings_yield::EarningsYieldFactor;
