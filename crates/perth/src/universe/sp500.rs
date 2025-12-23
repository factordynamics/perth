//! S&P 500 universe with GICS sector classifications.

use crate::universe::gics::GicsSector;
use std::collections::HashMap;

/// S&P 500 constituent with GICS sector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constituent {
    /// Stock symbol.
    pub symbol: String,
    /// GICS sector.
    pub sector: GicsSector,
}

impl Constituent {
    /// Create a new constituent.
    pub fn new(symbol: impl Into<String>, sector: GicsSector) -> Self {
        Self {
            symbol: symbol.into(),
            sector,
        }
    }
}

/// S&P 500 universe.
#[derive(Debug, Clone)]
pub struct SP500Universe {
    constituents: Vec<Constituent>,
    symbol_to_sector: HashMap<String, GicsSector>,
}

impl SP500Universe {
    /// Create a new S&P 500 universe with default constituents.
    pub fn new() -> Self {
        let constituents = Self::default_constituents();
        let symbol_to_sector = constituents
            .iter()
            .map(|c| (c.symbol.clone(), c.sector))
            .collect();

        Self {
            constituents,
            symbol_to_sector,
        }
    }

    /// Get all constituents.
    pub fn constituents(&self) -> &[Constituent] {
        &self.constituents
    }

    /// Get all symbols.
    pub fn symbols(&self) -> Vec<String> {
        self.constituents.iter().map(|c| c.symbol.clone()).collect()
    }

    /// Get the GICS sector for a symbol.
    pub fn sector(&self, symbol: &str) -> Option<GicsSector> {
        self.symbol_to_sector.get(symbol).copied()
    }

    /// Get all symbols in a specific sector.
    pub fn symbols_in_sector(&self, sector: GicsSector) -> Vec<String> {
        self.constituents
            .iter()
            .filter(|c| c.sector == sector)
            .map(|c| c.symbol.clone())
            .collect()
    }

    /// Get the count of constituents per sector.
    pub fn sector_counts(&self) -> HashMap<GicsSector, usize> {
        let mut counts = HashMap::new();
        for constituent in &self.constituents {
            *counts.entry(constituent.sector).or_insert(0) += 1;
        }
        counts
    }

    /// Default S&P 500 constituents (100+ stocks across all 11 GICS sectors).
    fn default_constituents() -> Vec<Constituent> {
        use GicsSector::*;

        vec![
            // Information Technology (45) - 15 stocks
            Constituent::new("AAPL", InformationTechnology),
            Constituent::new("MSFT", InformationTechnology),
            Constituent::new("NVDA", InformationTechnology),
            Constituent::new("AVGO", InformationTechnology),
            Constituent::new("ORCL", InformationTechnology),
            Constituent::new("CSCO", InformationTechnology),
            Constituent::new("ACN", InformationTechnology),
            Constituent::new("AMD", InformationTechnology),
            Constituent::new("IBM", InformationTechnology),
            Constituent::new("INTC", InformationTechnology),
            Constituent::new("TXN", InformationTechnology),
            Constituent::new("QCOM", InformationTechnology),
            Constituent::new("ADBE", InformationTechnology),
            Constituent::new("CRM", InformationTechnology),
            Constituent::new("NOW", InformationTechnology),
            // Health Care (35) - 12 stocks
            Constituent::new("LLY", HealthCare),
            Constituent::new("UNH", HealthCare),
            Constituent::new("JNJ", HealthCare),
            Constituent::new("ABBV", HealthCare),
            Constituent::new("MRK", HealthCare),
            Constituent::new("TMO", HealthCare),
            Constituent::new("ABT", HealthCare),
            Constituent::new("DHR", HealthCare),
            Constituent::new("PFE", HealthCare),
            Constituent::new("BMY", HealthCare),
            Constituent::new("AMGN", HealthCare),
            Constituent::new("GILD", HealthCare),
            // Financials (40) - 12 stocks
            Constituent::new("BRK.B", Financials),
            Constituent::new("JPM", Financials),
            Constituent::new("V", Financials),
            Constituent::new("MA", Financials),
            Constituent::new("BAC", Financials),
            Constituent::new("WFC", Financials),
            Constituent::new("MS", Financials),
            Constituent::new("GS", Financials),
            Constituent::new("BLK", Financials),
            Constituent::new("C", Financials),
            Constituent::new("AXP", Financials),
            Constituent::new("SCHW", Financials),
            // Consumer Discretionary (25) - 12 stocks
            Constituent::new("AMZN", ConsumerDiscretionary),
            Constituent::new("TSLA", ConsumerDiscretionary),
            Constituent::new("HD", ConsumerDiscretionary),
            Constituent::new("MCD", ConsumerDiscretionary),
            Constituent::new("NKE", ConsumerDiscretionary),
            Constituent::new("SBUX", ConsumerDiscretionary),
            Constituent::new("LOW", ConsumerDiscretionary),
            Constituent::new("TJX", ConsumerDiscretionary),
            Constituent::new("BKNG", ConsumerDiscretionary),
            Constituent::new("CMG", ConsumerDiscretionary),
            Constituent::new("F", ConsumerDiscretionary),
            Constituent::new("GM", ConsumerDiscretionary),
            // Communication Services (50) - 10 stocks
            Constituent::new("GOOGL", CommunicationServices),
            Constituent::new("GOOG", CommunicationServices),
            Constituent::new("META", CommunicationServices),
            Constituent::new("NFLX", CommunicationServices),
            Constituent::new("DIS", CommunicationServices),
            Constituent::new("CMCSA", CommunicationServices),
            Constituent::new("T", CommunicationServices),
            Constituent::new("VZ", CommunicationServices),
            Constituent::new("TMUS", CommunicationServices),
            Constituent::new("EA", CommunicationServices),
            // Industrials (20) - 12 stocks
            Constituent::new("CAT", Industrials),
            Constituent::new("UNP", Industrials),
            Constituent::new("RTX", Industrials),
            Constituent::new("HON", Industrials),
            Constituent::new("UPS", Industrials),
            Constituent::new("BA", Industrials),
            Constituent::new("DE", Industrials),
            Constituent::new("LMT", Industrials),
            Constituent::new("GE", Industrials),
            Constituent::new("MMM", Industrials),
            Constituent::new("FDX", Industrials),
            Constituent::new("NSC", Industrials),
            // Consumer Staples (30) - 10 stocks
            Constituent::new("WMT", ConsumerStaples),
            Constituent::new("PG", ConsumerStaples),
            Constituent::new("COST", ConsumerStaples),
            Constituent::new("KO", ConsumerStaples),
            Constituent::new("PEP", ConsumerStaples),
            Constituent::new("PM", ConsumerStaples),
            Constituent::new("MO", ConsumerStaples),
            Constituent::new("CL", ConsumerStaples),
            Constituent::new("MDLZ", ConsumerStaples),
            Constituent::new("KHC", ConsumerStaples),
            // Energy (10) - 10 stocks
            Constituent::new("XOM", Energy),
            Constituent::new("CVX", Energy),
            Constituent::new("COP", Energy),
            Constituent::new("SLB", Energy),
            Constituent::new("EOG", Energy),
            Constituent::new("MPC", Energy),
            Constituent::new("PSX", Energy),
            Constituent::new("VLO", Energy),
            Constituent::new("OXY", Energy),
            Constituent::new("HAL", Energy),
            // Utilities (55) - 8 stocks
            Constituent::new("NEE", Utilities),
            Constituent::new("SO", Utilities),
            Constituent::new("DUK", Utilities),
            Constituent::new("CEG", Utilities),
            Constituent::new("AEP", Utilities),
            Constituent::new("EXC", Utilities),
            Constituent::new("XEL", Utilities),
            Constituent::new("D", Utilities),
            // Real Estate (60) - 8 stocks
            Constituent::new("PLD", RealEstate),
            Constituent::new("AMT", RealEstate),
            Constituent::new("EQIX", RealEstate),
            Constituent::new("CCI", RealEstate),
            Constituent::new("PSA", RealEstate),
            Constituent::new("SPG", RealEstate),
            Constituent::new("O", RealEstate),
            Constituent::new("WELL", RealEstate),
            // Materials (15) - 10 stocks
            Constituent::new("LIN", Materials),
            Constituent::new("APD", Materials),
            Constituent::new("SHW", Materials),
            Constituent::new("FCX", Materials),
            Constituent::new("NEM", Materials),
            Constituent::new("ECL", Materials),
            Constituent::new("DD", Materials),
            Constituent::new("DOW", Materials),
            Constituent::new("PPG", Materials),
            Constituent::new("NUE", Materials),
        ]
    }
}

impl Default for SP500Universe {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_universe_creation() {
        let universe = SP500Universe::new();
        assert!(universe.constituents().len() >= 100);
        assert_eq!(universe.symbols().len(), universe.constituents().len());
    }

    #[test]
    fn test_all_sectors_represented() {
        let universe = SP500Universe::new();
        let sector_counts = universe.sector_counts();

        // Verify all 11 GICS sectors are represented
        for sector in GicsSector::all() {
            assert!(
                sector_counts.contains_key(&sector),
                "Sector {:?} not represented",
                sector
            );
        }
    }

    #[test]
    fn test_sector_lookup() {
        let universe = SP500Universe::new();

        assert_eq!(
            universe.sector("AAPL"),
            Some(GicsSector::InformationTechnology)
        );
        assert_eq!(universe.sector("XOM"), Some(GicsSector::Energy));
        assert_eq!(universe.sector("INVALID"), None);
    }

    #[test]
    fn test_symbols_in_sector() {
        let universe = SP500Universe::new();

        let tech_symbols = universe.symbols_in_sector(GicsSector::InformationTechnology);
        assert!(tech_symbols.contains(&"AAPL".to_string()));
        assert!(tech_symbols.contains(&"MSFT".to_string()));

        let energy_symbols = universe.symbols_in_sector(GicsSector::Energy);
        assert!(energy_symbols.contains(&"XOM".to_string()));
        assert!(energy_symbols.contains(&"CVX".to_string()));
    }

    #[test]
    fn test_sector_counts() {
        let universe = SP500Universe::new();
        let counts = universe.sector_counts();

        // Verify we have at least some stocks in each sector
        for sector in GicsSector::all() {
            let count = counts.get(&sector).unwrap();
            assert!(*count > 0, "Sector {:?} has no stocks", sector);
        }
    }
}
