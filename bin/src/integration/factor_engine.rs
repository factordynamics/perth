//! Factor computation engine using the factors crate.
//!
//! Computes factor scores for all securities in the universe using
//! the factors that can be computed from Yahoo Finance data alone.
//!
//! Uses shorter lookback windows to preserve more data for analysis.

use chrono::NaiveDate;
use factors::{
    ConfigurableFactor, Factor, Result as FactorResult, cross_sectional_standardize,
    liquidity::AmihudIlliquidity,
    momentum::{MediumTermMomentum, MediumTermMomentumConfig},
    volatility::{HistoricalVolatility, HistoricalVolatilityConfig, MarketBeta, MarketBetaConfig},
};
use polars::prelude::*;

/// Engine for computing all available factor scores.
///
/// Uses the following factors (computable from Yahoo data):
/// - Medium-Term Momentum (6-month lookback, 21-day skip)
/// - Size (log market cap) - computed directly from market_cap proxy
/// - Beta (systematic risk, 126-day window)
/// - Historical Volatility (63-day window)
/// - Amihud Illiquidity (21-day window)
///
/// Lookback windows are configured to balance signal quality with data availability.
pub(crate) struct FactorEngine {
    momentum: MediumTermMomentum,
    beta: MarketBeta,
    historical_vol: HistoricalVolatility,
    amihud: AmihudIlliquidity,
}

impl Default for FactorEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FactorEngine {
    /// Create a new factor engine with optimized configurations.
    ///
    /// Uses shorter lookback windows than defaults to preserve more data:
    /// - Momentum: 126 days (6 months) + 21-day skip = 147 days required
    /// - Beta: 126-day window with 40 min periods = 40 days required
    /// - Volatility: 63-day window with 20 min periods = 20 days required
    /// - Amihud: 21-day window with 10 min periods = 10 days required
    pub(crate) fn new() -> Self {
        // Use medium-term momentum (6 months) instead of composite
        let momentum = MediumTermMomentum::with_config(MediumTermMomentumConfig {
            lookback: 126,
            skip_days: 21,
        });

        // Use 126-day beta window (6 months) instead of default 252
        let beta = MarketBeta::with_config(MarketBetaConfig {
            lookback: 126,
            min_periods: 40,
        });

        // Use default 63-day volatility window
        let historical_vol =
            HistoricalVolatility::with_config(HistoricalVolatilityConfig::default());

        Self {
            momentum,
            beta,
            historical_vol,
            amihud: AmihudIlliquidity::default(),
        }
    }

    /// List of factors that can be computed.
    pub(crate) fn available_factors(&self) -> Vec<&str> {
        vec![
            self.momentum.name(),
            "log_market_cap", // Computed directly from market_cap proxy
            self.beta.name(),
            self.historical_vol.name(),
            self.amihud.name(),
        ]
    }

    /// Compute all factor scores for the universe.
    ///
    /// # Arguments
    /// * `data` - DataFrame with columns: date, symbol, adjusted_close (as close),
    ///   market_return, market_cap, volume
    /// * `date` - The target date for factor computation
    ///
    /// # Returns
    /// DataFrame with columns: date, symbol, momentum_score, size_score, beta_score,
    /// volatility_score, amihud_score
    pub(crate) fn compute_all_scores(
        &self,
        data: &DataFrame,
        date: NaiveDate,
    ) -> FactorResult<DataFrame> {
        // Prepare input data for momentum (needs: symbol, date, close)
        let momentum_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("adjusted_close").alias("close"),
        ]);
        let momentum_scores = self.momentum.compute(&momentum_input, date)?;

        // Compute size factor directly from market_cap proxy
        // Since Yahoo data doesn't provide shares_outstanding, we use our market_cap proxy
        // and compute log(market_cap) with cross-sectional standardization
        let size_scores = self.compute_size_factor(data, date)?;

        // Prepare input for beta (needs: symbol, date, close, market_return)
        let beta_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("adjusted_close").alias("close"),
            col("market_return"),
        ]);
        let beta_scores = self.beta.compute(&beta_input, date)?;

        // Prepare input for historical volatility (needs: symbol, date, close)
        let vol_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("adjusted_close").alias("close"),
        ]);
        let vol_scores = self.historical_vol.compute(&vol_input, date)?;

        // Prepare input for amihud (needs: symbol, date, close, volume)
        let amihud_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("adjusted_close").alias("close"),
            col("volume").cast(DataType::Float64),
        ]);
        let amihud_scores = self.amihud.compute(&amihud_input, date)?;

        // Join all scores on (date, symbol)
        let combined = momentum_scores
            .lazy()
            .join(
                size_scores.lazy(),
                [col("date"), col("symbol")],
                [col("date"), col("symbol")],
                JoinArgs::new(JoinType::Inner),
            )
            .join(
                beta_scores.lazy(),
                [col("date"), col("symbol")],
                [col("date"), col("symbol")],
                JoinArgs::new(JoinType::Inner),
            )
            .join(
                vol_scores.lazy(),
                [col("date"), col("symbol")],
                [col("date"), col("symbol")],
                JoinArgs::new(JoinType::Inner),
            )
            .join(
                amihud_scores.lazy(),
                [col("date"), col("symbol")],
                [col("date"), col("symbol")],
                JoinArgs::new(JoinType::Inner),
            )
            .collect()?;

        Ok(combined)
    }

    /// Compute size factor from market_cap proxy.
    ///
    /// Uses log(market_cap) with cross-sectional standardization.
    /// This handles the case where we don't have shares_outstanding from Yahoo data.
    fn compute_size_factor(&self, data: &DataFrame, date: NaiveDate) -> FactorResult<DataFrame> {
        let date_str = date.format("%Y-%m-%d").to_string();

        let raw_scores = data
            .clone()
            .lazy()
            .filter(col("date").eq(lit(date_str)))
            .with_column(
                col("market_cap")
                    .log(std::f64::consts::E)
                    .alias("log_market_cap"),
            )
            .select([col("symbol"), col("date"), col("log_market_cap")])
            .filter(col("log_market_cap").is_not_null())
            .collect()?;

        // Apply cross-sectional standardization
        cross_sectional_standardize(&raw_scores, "log_market_cap")
    }
}
