//! Integration tests for XBRL parsing

use chrono::NaiveDate;
use perth_data::edgar::xbrl::{XbrlDocument, XbrlFact, concepts};

#[test]
fn test_xbrl_document_creation() {
    let doc = XbrlDocument::new();
    assert_eq!(doc.facts.len(), 0);
    assert!(doc.entity_name.is_none());
    assert!(doc.cik.is_none());
}

#[test]
fn test_xbrl_fact_instant_vs_duration() {
    // Instant fact (balance sheet)
    let instant_fact = XbrlFact {
        concept: concepts::balance_sheet::ASSETS.to_string(),
        value: 1000000.0,
        unit: "USD".to_string(),
        period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
        period_start: None,
        form: Some("10-K".to_string()),
        fiscal_year: Some(2023),
        fiscal_period: Some("FY".to_string()),
    };

    assert!(instant_fact.is_instant());
    assert!(!instant_fact.is_duration());
    assert!(instant_fact.duration_days().is_none());

    // Duration fact (income statement)
    let duration_fact = XbrlFact {
        concept: concepts::income_statement::NET_INCOME.to_string(),
        value: 100000.0,
        unit: "USD".to_string(),
        period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
        period_start: Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
        form: Some("10-K".to_string()),
        fiscal_year: Some(2023),
        fiscal_period: Some("FY".to_string()),
    };

    assert!(!duration_fact.is_instant());
    assert!(duration_fact.is_duration());
    assert_eq!(duration_fact.duration_days(), Some(364));
}

#[test]
fn test_xbrl_document_query_methods() {
    let mut doc = XbrlDocument::new();

    // Add multiple facts
    doc.facts.push(XbrlFact {
        concept: concepts::balance_sheet::ASSETS.to_string(),
        value: 1000000.0,
        unit: "USD".to_string(),
        period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
        period_start: None,
        form: Some("10-K".to_string()),
        fiscal_year: Some(2023),
        fiscal_period: Some("FY".to_string()),
    });

    doc.facts.push(XbrlFact {
        concept: concepts::balance_sheet::ASSETS.to_string(),
        value: 950000.0,
        unit: "USD".to_string(),
        period_end: NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
        period_start: None,
        form: Some("10-K".to_string()),
        fiscal_year: Some(2022),
        fiscal_period: Some("FY".to_string()),
    });

    doc.facts.push(XbrlFact {
        concept: concepts::income_statement::NET_INCOME.to_string(),
        value: 100000.0,
        unit: "USD".to_string(),
        period_end: NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
        period_start: Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
        form: Some("10-K".to_string()),
        fiscal_year: Some(2023),
        fiscal_period: Some("FY".to_string()),
    });

    // Test get_latest_fact
    let latest = doc
        .get_latest_fact(concepts::balance_sheet::ASSETS)
        .unwrap();
    assert_eq!(latest.value, 1000000.0);
    assert_eq!(latest.fiscal_year, Some(2023));

    // Test get_fact with specific date
    let specific = doc
        .get_fact(
            concepts::balance_sheet::ASSETS,
            NaiveDate::from_ymd_opt(2022, 12, 31).unwrap(),
        )
        .unwrap();
    assert_eq!(specific.value, 950000.0);

    // Test get_facts_by_concept
    let all_assets = doc.get_facts_by_concept(concepts::balance_sheet::ASSETS);
    assert_eq!(all_assets.len(), 2);
    // Should be sorted newest first
    assert_eq!(all_assets[0].fiscal_year, Some(2023));
    assert_eq!(all_assets[1].fiscal_year, Some(2022));

    // Test get_facts_by_fiscal_year
    let fy2023 = doc.get_facts_by_fiscal_year(concepts::balance_sheet::ASSETS, 2023);
    assert_eq!(fy2023.len(), 1);
    assert_eq!(fy2023[0].value, 1000000.0);

    // Test get_facts_by_form
    let tenk_assets = doc.get_facts_by_form(concepts::balance_sheet::ASSETS, "10-K");
    assert_eq!(tenk_assets.len(), 2);

    // Test get_concepts
    let concepts = doc.get_concepts();
    assert_eq!(concepts.len(), 2);
    assert!(concepts.contains(&concepts::balance_sheet::ASSETS.to_string()));
    assert!(concepts.contains(&concepts::income_statement::NET_INCOME.to_string()));
}

#[test]
fn test_parse_json_minimal() {
    // Test with minimal valid JSON structure
    let json = r#"{
        "cik": "0000320193",
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Assets": {
                    "label": "Assets",
                    "description": "Sum of the carrying amounts...",
                    "units": {
                        "USD": [
                            {
                                "end": "2023-09-30",
                                "val": 352755000000.0,
                                "accn": "0000320193-23-000077",
                                "fy": 2023,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2023-11-03"
                            }
                        ]
                    }
                }
            }
        }
    }"#;

    let doc = XbrlDocument::parse_json(json).unwrap();

    assert_eq!(doc.entity_name, Some("Apple Inc.".to_string()));
    assert_eq!(doc.cik, Some("0000320193".to_string()));
    assert_eq!(doc.facts.len(), 1);

    let fact = &doc.facts[0];
    assert_eq!(fact.concept, "us-gaap:Assets");
    assert_eq!(fact.value, 352755000000.0);
    assert_eq!(fact.unit, "USD");
    assert_eq!(
        fact.period_end,
        NaiveDate::from_ymd_opt(2023, 9, 30).unwrap()
    );
    assert!(fact.period_start.is_none());
    assert_eq!(fact.form, Some("10-K".to_string()));
    assert_eq!(fact.fiscal_year, Some(2023));
    assert_eq!(fact.fiscal_period, Some("FY".to_string()));
}

#[test]
fn test_parse_json_with_duration() {
    // Test with a duration fact (has start and end dates)
    let json = r#"{
        "cik": "0000320193",
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "label": "Net Income (Loss)",
                    "description": "The portion of profit or loss...",
                    "units": {
                        "USD": [
                            {
                                "start": "2022-09-26",
                                "end": "2023-09-30",
                                "val": 96995000000.0,
                                "accn": "0000320193-23-000077",
                                "fy": 2023,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2023-11-03"
                            }
                        ]
                    }
                }
            }
        }
    }"#;

    let doc = XbrlDocument::parse_json(json).unwrap();

    assert_eq!(doc.facts.len(), 1);

    let fact = &doc.facts[0];
    assert_eq!(fact.concept, "us-gaap:NetIncomeLoss");
    assert_eq!(fact.value, 96995000000.0);
    assert_eq!(
        fact.period_start,
        Some(NaiveDate::from_ymd_opt(2022, 9, 26).unwrap())
    );
    assert_eq!(
        fact.period_end,
        NaiveDate::from_ymd_opt(2023, 9, 30).unwrap()
    );
    assert!(fact.is_duration());
}

#[test]
fn test_parse_json_invalid() {
    let result = XbrlDocument::parse_json("invalid json");
    assert!(result.is_err());

    let result = XbrlDocument::parse_json("{}");
    assert!(result.is_err());
}

#[test]
fn test_concepts_constants() {
    // Balance sheet
    assert_eq!(concepts::balance_sheet::ASSETS, "us-gaap:Assets");
    assert_eq!(concepts::balance_sheet::LIABILITIES, "us-gaap:Liabilities");
    assert_eq!(
        concepts::balance_sheet::STOCKHOLDERS_EQUITY,
        "us-gaap:StockholdersEquity"
    );

    // Income statement
    assert_eq!(
        concepts::income_statement::NET_INCOME,
        "us-gaap:NetIncomeLoss"
    );
    assert_eq!(concepts::income_statement::REVENUES, "us-gaap:Revenues");

    // Cash flow
    assert_eq!(
        concepts::cash_flow::OPERATING_CASH_FLOW,
        "us-gaap:NetCashProvidedByUsedInOperatingActivities"
    );

    // Per-share
    assert_eq!(
        concepts::per_share::EPS_BASIC,
        "us-gaap:EarningsPerShareBasic"
    );
}
