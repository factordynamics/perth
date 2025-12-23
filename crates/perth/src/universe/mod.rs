//! Universe management for Perth factor model.
//!
//! This module provides functionality for managing stock universes,
//! including the S&P 500 and GICS sector classifications.

pub mod gics;
pub mod sp500;

pub use gics::GicsSector;
pub use sp500::{Constituent, SP500Universe};

/// Trait for stock universes.
pub trait Universe {
    /// Get all symbols in the universe.
    fn symbols(&self) -> Vec<String>;

    /// Check if a symbol is in the universe.
    fn contains(&self, symbol: &str) -> bool {
        self.symbols().contains(&symbol.to_string())
    }

    /// Get the number of constituents.
    fn size(&self) -> usize {
        self.symbols().len()
    }
}

impl Universe for SP500Universe {
    fn symbols(&self) -> Vec<String> {
        self.symbols()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_universe_trait() {
        let universe = SP500Universe::new();

        assert!(universe.contains("AAPL"));
        assert!(!universe.contains("NOTREAL"));
        assert!(universe.size() >= 100);
    }
}
