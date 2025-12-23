//! Factor computation engine using perth-factors.
//!
//! Computes factor scores for all securities in the universe using
//! the factors that can be computed from Yahoo Finance data alone.
//!
//! Uses shorter lookback windows to preserve more data for analysis.

use polars::prelude::*;
use toraniko_traits::{Factor, FactorError, StyleFactor};

use perth_factors::liquidity::AmihudFactor;
use perth_factors::momentum::MediumTermMomentumFactor;
use perth_factors::momentum::medium_term::MediumTermMomentumConfig;
use perth_factors::size::LogMarketCapFactor;
use perth_factors::volatility::beta::BetaConfig;
use perth_factors::volatility::historical_vol::HistoricalVolatilityConfig;
use perth_factors::volatility::{BetaFactor, HistoricalVolatilityFactor};

/// Engine for computing all available factor scores.
///
/// Uses the following factors (computable from Yahoo data):
/// - Medium-Term Momentum (6-month lookback, 21-day skip)
/// - Size (log market cap)
/// - Beta (systematic risk, 126-day window)
/// - Historical Volatility (63-day window)
/// - Amihud Illiquidity (21-day window)
///
/// Lookback windows are configured to balance signal quality with data availability.
pub(crate) struct FactorEngine {
    momentum: MediumTermMomentumFactor,
    size: LogMarketCapFactor,
    beta: BetaFactor,
    historical_vol: HistoricalVolatilityFactor,
    amihud: AmihudFactor,
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
        let momentum = MediumTermMomentumFactor::with_config(MediumTermMomentumConfig {
            lookback: 126,
            skip_days: 21,
        });

        // Use 126-day beta window (6 months) instead of default 252
        let beta = BetaFactor::with_config(BetaConfig {
            window: 126,
            min_periods: 40,
            market_column: "market_return".to_string(),
        });

        // Use default 63-day volatility window
        let historical_vol =
            HistoricalVolatilityFactor::with_config(HistoricalVolatilityConfig::default());

        Self {
            momentum,
            size: LogMarketCapFactor::default(),
            beta,
            historical_vol,
            amihud: AmihudFactor::default(),
        }
    }

    /// List of factors that can be computed.
    pub(crate) fn available_factors(&self) -> Vec<&str> {
        vec![
            self.momentum.name(),
            self.size.name(),
            self.beta.name(),
            self.historical_vol.name(),
            self.amihud.name(),
        ]
    }

    /// Compute all factor scores for the universe.
    ///
    /// # Arguments
    /// * `data` - DataFrame with columns: date, symbol, adjusted_close (as price),
    ///   asset_returns (as returns), market_return, market_cap, volume
    ///
    /// # Returns
    /// DataFrame with columns: date, symbol, momentum_score, size_score, beta_score,
    /// volatility_score, amihud_score
    pub(crate) fn compute_all_scores(&self, data: &DataFrame) -> Result<DataFrame, FactorError> {
        // Prepare input data for momentum (needs: symbol, date, price, returns)
        let momentum_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("adjusted_close").alias("price"),
            col("asset_returns").alias("returns"),
        ]);
        let momentum_scores = self
            .momentum
            .compute_scores(momentum_input)?
            .collect()
            .map_err(FactorError::Computation)?;

        // Prepare input for size (needs: symbol, date, market_cap)
        let size_input =
            data.clone()
                .lazy()
                .select([col("symbol"), col("date"), col("market_cap")]);
        let size_scores = self
            .size
            .compute_scores(size_input)?
            .collect()
            .map_err(FactorError::Computation)?;

        // Prepare input for beta (needs: symbol, date, returns, market_return)
        let beta_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("asset_returns").alias("returns"),
            col("market_return"),
        ]);
        let beta_scores = self
            .beta
            .compute_scores(beta_input)?
            .collect()
            .map_err(FactorError::Computation)?;

        // Prepare input for historical volatility (needs: symbol, date, returns)
        let vol_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("asset_returns").alias("returns"),
        ]);
        let vol_scores = self
            .historical_vol
            .compute_scores(vol_input)?
            .collect()
            .map_err(FactorError::Computation)?;

        // Prepare input for amihud (needs: symbol, date, returns, price, volume)
        let amihud_input = data.clone().lazy().select([
            col("symbol"),
            col("date"),
            col("asset_returns").alias("returns"),
            col("adjusted_close").alias("price"),
            col("volume").cast(DataType::Float64),
        ]);
        let amihud_scores = self
            .amihud
            .compute_scores(amihud_input)?
            .collect()
            .map_err(FactorError::Computation)?;

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
            .collect()
            .map_err(FactorError::Computation)?;

        Ok(combined)
    }
}
