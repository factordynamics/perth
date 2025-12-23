//! GICS (Global Industry Classification Standard) sector definitions.

use serde::{Deserialize, Serialize};
use std::fmt;

/// GICS Level 1 sectors (11 sectors).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GicsSector {
    /// Information Technology
    InformationTechnology,

    /// Health Care
    HealthCare,

    /// Financials
    Financials,

    /// Consumer Discretionary
    ConsumerDiscretionary,

    /// Communication Services
    CommunicationServices,

    /// Industrials
    Industrials,

    /// Consumer Staples
    ConsumerStaples,

    /// Energy
    Energy,

    /// Utilities
    Utilities,

    /// Real Estate
    RealEstate,

    /// Materials
    Materials,
}

impl GicsSector {
    /// Returns all GICS sectors.
    pub fn all() -> Vec<Self> {
        vec![
            Self::InformationTechnology,
            Self::HealthCare,
            Self::Financials,
            Self::ConsumerDiscretionary,
            Self::CommunicationServices,
            Self::Industrials,
            Self::ConsumerStaples,
            Self::Energy,
            Self::Utilities,
            Self::RealEstate,
            Self::Materials,
        ]
    }

    /// Returns the sector code (2-digit).
    pub const fn code(&self) -> u8 {
        match self {
            Self::Energy => 10,
            Self::Materials => 15,
            Self::Industrials => 20,
            Self::ConsumerDiscretionary => 25,
            Self::ConsumerStaples => 30,
            Self::HealthCare => 35,
            Self::Financials => 40,
            Self::InformationTechnology => 45,
            Self::CommunicationServices => 50,
            Self::Utilities => 55,
            Self::RealEstate => 60,
        }
    }

    /// Returns the full sector name.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::InformationTechnology => "Information Technology",
            Self::HealthCare => "Health Care",
            Self::Financials => "Financials",
            Self::ConsumerDiscretionary => "Consumer Discretionary",
            Self::CommunicationServices => "Communication Services",
            Self::Industrials => "Industrials",
            Self::ConsumerStaples => "Consumer Staples",
            Self::Energy => "Energy",
            Self::Utilities => "Utilities",
            Self::RealEstate => "Real Estate",
            Self::Materials => "Materials",
        }
    }

    /// Parse a sector from its code.
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            10 => Some(Self::Energy),
            15 => Some(Self::Materials),
            20 => Some(Self::Industrials),
            25 => Some(Self::ConsumerDiscretionary),
            30 => Some(Self::ConsumerStaples),
            35 => Some(Self::HealthCare),
            40 => Some(Self::Financials),
            45 => Some(Self::InformationTechnology),
            50 => Some(Self::CommunicationServices),
            55 => Some(Self::Utilities),
            60 => Some(Self::RealEstate),
            _ => None,
        }
    }
}

impl fmt::Display for GicsSector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_sectors() {
        let sectors = GicsSector::all();
        assert_eq!(sectors.len(), 11);
    }

    #[test]
    fn test_sector_codes() {
        assert_eq!(GicsSector::Energy.code(), 10);
        assert_eq!(GicsSector::InformationTechnology.code(), 45);
        assert_eq!(GicsSector::RealEstate.code(), 60);
    }

    #[test]
    fn test_from_code() {
        assert_eq!(
            GicsSector::from_code(45),
            Some(GicsSector::InformationTechnology)
        );
        assert_eq!(GicsSector::from_code(10), Some(GicsSector::Energy));
        assert_eq!(GicsSector::from_code(99), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(
            format!("{}", GicsSector::InformationTechnology),
            "Information Technology"
        );
        assert_eq!(format!("{}", GicsSector::Energy), "Energy");
    }
}
