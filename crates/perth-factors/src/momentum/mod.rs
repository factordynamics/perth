//! Momentum factors - measures of trend persistence
//!
//! Momentum factors capture the tendency of securities with strong recent performance
//! to continue outperforming. Different lookback windows capture short, medium, and
//! long-term momentum effects.

pub mod composite;
pub mod long_term;
pub mod medium_term;
pub mod short_term;

pub use composite::CompositeMomentumFactor;
pub use long_term::LongTermMomentumFactor;
pub use medium_term::MediumTermMomentumFactor;
pub use short_term::ShortTermMomentumFactor;
