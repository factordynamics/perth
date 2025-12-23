//! GICS sector one-hot encoding.
//!
//! Converts sector assignments from SP500Universe into one-hot encoded
//! columns for use in cross-sectional factor regression.

use perth::universe::{GicsSector, SP500Universe};
use polars::prelude::*;

/// Generate sector column name for a GICS sector.
fn sector_column_name(sector: GicsSector) -> String {
    format!("sector_{}", sector.name().replace(' ', "_"))
}

/// Encode GICS sectors as one-hot columns.
///
/// For each symbol in the quotes DataFrame, looks up its sector from the
/// SP500Universe and creates one-hot encoded columns for all 11 GICS sectors.
///
/// Returns DataFrame with columns: [date, symbol, sector_Information_Technology, sector_Health_Care, ...]
pub(crate) fn encode_gics_sectors(
    universe: &SP500Universe,
    quotes: &DataFrame,
) -> Result<DataFrame, PolarsError> {
    // Start with date and symbol columns
    let mut lf = quotes.clone().lazy().select([col("date"), col("symbol")]);

    // Add a sector column based on universe lookup
    // First, we need to create a mapping DataFrame
    let symbols: Vec<String> = universe.symbols();
    let sectors: Vec<String> = symbols
        .iter()
        .map(|s| {
            universe
                .sector(s)
                .map(|sec| sec.name().to_string())
                .unwrap_or_else(|| "Other".to_string())
        })
        .collect();

    let sector_map = DataFrame::new(vec![
        Column::new("symbol".into(), symbols),
        Column::new("sector".into(), sectors),
    ])?;

    // Join to get sector for each row
    lf = lf.join(
        sector_map.lazy(),
        [col("symbol")],
        [col("symbol")],
        JoinArgs::new(JoinType::Left),
    );

    // Create one-hot encoded columns for each GICS sector
    for sector in GicsSector::all() {
        let col_name = sector_column_name(sector);
        let sector_name = sector.name();

        lf = lf.with_column(
            when(col("sector").eq(lit(sector_name)))
                .then(lit(1.0))
                .otherwise(lit(0.0))
                .alias(&col_name),
        );
    }

    // Remove the intermediate sector column, keep only one-hot columns
    let sector_cols: Vec<Expr> = std::iter::once(col("date"))
        .chain(std::iter::once(col("symbol")))
        .chain(
            GicsSector::all()
                .into_iter()
                .map(|s| col(sector_column_name(s))),
        )
        .collect();

    let result = lf.select(sector_cols).collect()?;

    Ok(result)
}
