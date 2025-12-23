//! SQLite caching layer for market data.

use crate::error::{DataError, Result};
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// SQLite cache for market data.
#[derive(Debug)]
pub struct SqliteCache {
    conn: Connection,
}

/// Period type for financial statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeriodType {
    /// Quarterly report (10-Q)
    Quarterly,
    /// Annual report (10-K)
    Annual,
}

impl PeriodType {
    /// Convert to database string representation.
    pub const fn to_db_str(&self) -> &'static str {
        match self {
            Self::Quarterly => "Q",
            Self::Annual => "A",
        }
    }

    /// Parse from database string representation.
    pub fn from_db_str(s: &str) -> Result<Self> {
        match s {
            "Q" => Ok(Self::Quarterly),
            "A" => Ok(Self::Annual),
            _ => Err(DataError::Parse(format!("Invalid period type: {}", s))),
        }
    }
}

/// Financial statement data from SEC EDGAR.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinancialStatement {
    /// Stock symbol
    pub symbol: String,
    /// Company CIK
    pub cik: String,
    /// Period end date
    pub period_end: NaiveDate,
    /// Period type (quarterly or annual)
    pub period_type: PeriodType,
    /// Fiscal year
    pub fiscal_year: i32,
    /// Fiscal quarter (1-4 for quarterly, None for annual)
    pub fiscal_quarter: Option<i32>,

    // Balance Sheet
    /// Total assets
    pub total_assets: Option<f64>,
    /// Total liabilities
    pub total_liabilities: Option<f64>,
    /// Stockholders equity
    pub stockholders_equity: Option<f64>,
    /// Long-term debt
    pub long_term_debt: Option<f64>,
    /// Current assets
    pub current_assets: Option<f64>,
    /// Current liabilities
    pub current_liabilities: Option<f64>,
    /// Cash and equivalents
    pub cash_and_equivalents: Option<f64>,

    // Income Statement
    /// Revenue
    pub revenue: Option<f64>,
    /// Net income
    pub net_income: Option<f64>,
    /// Operating income
    pub operating_income: Option<f64>,
    /// Gross profit
    pub gross_profit: Option<f64>,
    /// EPS (basic)
    pub eps_basic: Option<f64>,
    /// EPS (diluted)
    pub eps_diluted: Option<f64>,

    // Cash Flow
    /// Operating cash flow
    pub operating_cash_flow: Option<f64>,
    /// Capital expenditures
    pub capital_expenditures: Option<f64>,
    /// Free cash flow
    pub free_cash_flow: Option<f64>,

    // Shares
    /// Shares outstanding
    pub shares_outstanding: Option<f64>,
    /// Shares outstanding (diluted)
    pub shares_outstanding_diluted: Option<f64>,

    /// When this data was cached
    pub cached_at: DateTime<Utc>,
}

impl SqliteCache {
    /// Create a new SQLite cache.
    ///
    /// # Arguments
    /// * `path` - Path to the SQLite database file
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let cache = Self { conn };
        cache.initialize_schema()?;
        Ok(cache)
    }

    /// Create an in-memory cache (useful for testing).
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let cache = Self { conn };
        cache.initialize_schema()?;
        Ok(cache)
    }

    /// Initialize the database schema.
    fn initialize_schema(&self) -> Result<()> {
        // Quotes table
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS quotes (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                adjusted_close REAL NOT NULL,
                cached_at TEXT NOT NULL,
                PRIMARY KEY (symbol, date)
            )",
            [],
        )?;

        // Create index on symbol and date
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quotes_symbol_date ON quotes(symbol, date)",
            [],
        )?;

        // Universe table (list of symbols to track)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS universe (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                sector TEXT,
                industry TEXT,
                added_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )",
            [],
        )?;

        // Market cap table
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS market_caps (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                market_cap REAL NOT NULL,
                cached_at TEXT NOT NULL,
                PRIMARY KEY (symbol, date)
            )",
            [],
        )?;

        // Fundamentals table
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS fundamentals (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                data TEXT NOT NULL,
                cached_at TEXT NOT NULL,
                PRIMARY KEY (symbol, date)
            )",
            [],
        )?;

        // Company CIK mappings
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS company_ciks (
                symbol TEXT PRIMARY KEY,
                cik TEXT NOT NULL,
                company_name TEXT,
                updated_at TEXT NOT NULL
            )",
            [],
        )?;

        // Financial statements cache
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS financial_statements (
                symbol TEXT NOT NULL,
                cik TEXT NOT NULL,
                period_end TEXT NOT NULL,
                period_type TEXT NOT NULL,
                fiscal_year INTEGER NOT NULL,
                fiscal_quarter INTEGER,

                total_assets REAL,
                total_liabilities REAL,
                stockholders_equity REAL,
                long_term_debt REAL,
                current_assets REAL,
                current_liabilities REAL,
                cash_and_equivalents REAL,

                revenue REAL,
                net_income REAL,
                operating_income REAL,
                gross_profit REAL,
                eps_basic REAL,
                eps_diluted REAL,

                operating_cash_flow REAL,
                capital_expenditures REAL,
                free_cash_flow REAL,

                shares_outstanding REAL,
                shares_outstanding_diluted REAL,

                cached_at TEXT NOT NULL,
                PRIMARY KEY (symbol, period_end, period_type)
            )",
            [],
        )?;

        // Create indices for financial statements
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_financials_symbol ON financial_statements(symbol)",
            [],
        )?;

        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_financials_period ON financial_statements(period_end)",
            [],
        )?;

        Ok(())
    }

    /// Check if quotes are cached for a symbol and date range.
    pub fn has_quotes(&self, symbol: &str, start: NaiveDate, end: NaiveDate) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM quotes
             WHERE symbol = ?1 AND date >= ?2 AND date <= ?3",
            params![symbol, start.to_string(), end.to_string()],
            |row| row.get(0),
        )?;

        // Check if we have data for most of the expected trading days
        // Roughly 252 trading days per year, so ~21 per month
        let days = (end - start).num_days();
        let expected_count = (days as f64 * 0.7) as i64; // 70% of calendar days

        Ok(count >= expected_count)
    }

    /// Get cached quotes for a symbol and date range.
    pub fn get_quotes(&self, symbol: &str, start: NaiveDate, end: NaiveDate) -> Result<DataFrame> {
        let mut stmt = self.conn.prepare(
            "SELECT symbol, date, open, high, low, close, volume, adjusted_close
             FROM quotes
             WHERE symbol = ?1 AND date >= ?2 AND date <= ?3
             ORDER BY date ASC",
        )?;

        let mut symbols = Vec::new();
        let mut dates = Vec::new();
        let mut opens = Vec::new();
        let mut highs = Vec::new();
        let mut lows = Vec::new();
        let mut closes = Vec::new();
        let mut volumes = Vec::new();
        let mut adj_closes = Vec::new();

        let rows = stmt.query_map(params![symbol, start.to_string(), end.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, f64>(7)?,
            ))
        })?;

        for row in rows {
            let (sym, date, open, high, low, close, volume, adj_close) = row?;
            symbols.push(sym);
            dates.push(date);
            opens.push(open);
            highs.push(high);
            lows.push(low);
            closes.push(close);
            volumes.push(volume as u64);
            adj_closes.push(adj_close);
        }

        if dates.is_empty() {
            return Err(DataError::MissingData {
                symbol: symbol.to_string(),
                reason: "No cached data found".to_string(),
            });
        }

        let df = DataFrame::new(vec![
            Series::new("symbol".into(), symbols).into(),
            Series::new("date".into(), dates).into(),
            Series::new("open".into(), opens).into(),
            Series::new("high".into(), highs).into(),
            Series::new("low".into(), lows).into(),
            Series::new("close".into(), closes).into(),
            Series::new("volume".into(), volumes).into(),
            Series::new("adjusted_close".into(), adj_closes).into(),
        ])?;

        // Convert date strings to Date type
        let df = df
            .lazy()
            .with_column(col("date").cast(DataType::Date))
            .collect()?;

        Ok(df)
    }

    /// Store quotes in the cache.
    pub fn put_quotes(&self, df: &DataFrame) -> Result<()> {
        let cached_at = Utc::now().to_rfc3339();

        // Get columns
        let symbols = df.column("symbol")?.str()?;
        let dates = df.column("date")?.cast(&DataType::String)?;
        let dates = dates.str()?;
        let opens = df.column("open")?.f64()?;
        let highs = df.column("high")?.f64()?;
        let lows = df.column("low")?.f64()?;
        let closes = df.column("close")?.f64()?;
        let volumes = df.column("volume")?.cast(&DataType::Int64)?;
        let volumes = volumes.i64()?;
        let adj_closes = df.column("adjusted_close")?.f64()?;

        let tx = self.conn.unchecked_transaction()?;

        for i in 0..df.height() {
            let symbol = symbols
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing symbol".to_string()))?;
            let date = dates
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing date".to_string()))?;
            let open = opens
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing open".to_string()))?;
            let high = highs
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing high".to_string()))?;
            let low = lows
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing low".to_string()))?;
            let close = closes
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing close".to_string()))?;
            let volume = volumes
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing volume".to_string()))?;
            let adj_close = adj_closes
                .get(i)
                .ok_or_else(|| DataError::Parse("Missing adjusted_close".to_string()))?;

            tx.execute(
                "INSERT OR REPLACE INTO quotes
                 (symbol, date, open, high, low, close, volume, adjusted_close, cached_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    symbol, date, open, high, low, close, volume, adj_close, cached_at
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// Add a symbol to the universe.
    pub fn add_to_universe(
        &self,
        symbol: &str,
        name: Option<&str>,
        sector: Option<&str>,
        industry: Option<&str>,
    ) -> Result<()> {
        let added_at = Utc::now().to_rfc3339();

        self.conn.execute(
            "INSERT OR REPLACE INTO universe (symbol, name, sector, industry, added_at, active)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)",
            params![symbol, name, sector, industry, added_at],
        )?;

        Ok(())
    }

    /// Get all active symbols in the universe.
    pub fn get_universe(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT symbol FROM universe WHERE active = 1 ORDER BY symbol")?;

        let symbols = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;

        Ok(symbols)
    }

    /// Remove a symbol from the universe (mark as inactive).
    pub fn remove_from_universe(&self, symbol: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE universe SET active = 0 WHERE symbol = ?1",
            params![symbol],
        )?;
        Ok(())
    }

    /// Store market cap data.
    pub fn put_market_cap(&self, symbol: &str, date: NaiveDate, market_cap: f64) -> Result<()> {
        let cached_at = Utc::now().to_rfc3339();

        self.conn.execute(
            "INSERT OR REPLACE INTO market_caps (symbol, date, market_cap, cached_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![symbol, date.to_string(), market_cap, cached_at],
        )?;

        Ok(())
    }

    /// Get market cap for a symbol on a specific date.
    pub fn get_market_cap(&self, symbol: &str, date: NaiveDate) -> Result<Option<f64>> {
        let result = self
            .conn
            .query_row(
                "SELECT market_cap FROM market_caps WHERE symbol = ?1 AND date = ?2",
                params![symbol, date.to_string()],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result)
    }

    /// Store fundamental data (as JSON).
    pub fn put_fundamentals(&self, symbol: &str, date: NaiveDate, data: &str) -> Result<()> {
        let cached_at = Utc::now().to_rfc3339();

        self.conn.execute(
            "INSERT OR REPLACE INTO fundamentals (symbol, date, data, cached_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![symbol, date.to_string(), data, cached_at],
        )?;

        Ok(())
    }

    /// Get fundamental data for a symbol on a specific date.
    pub fn get_fundamentals(&self, symbol: &str, date: NaiveDate) -> Result<Option<String>> {
        let result = self
            .conn
            .query_row(
                "SELECT data FROM fundamentals WHERE symbol = ?1 AND date = ?2",
                params![symbol, date.to_string()],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result)
    }

    /// Get CIK for a symbol.
    pub fn get_cik(&self, symbol: &str) -> Result<Option<String>> {
        let result = self
            .conn
            .query_row(
                "SELECT cik FROM company_ciks WHERE symbol = ?1",
                params![symbol],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result)
    }

    /// Store CIK mapping for a symbol.
    pub fn put_cik(&self, symbol: &str, cik: &str, company_name: Option<&str>) -> Result<()> {
        let updated_at = Utc::now().to_rfc3339();

        self.conn.execute(
            "INSERT OR REPLACE INTO company_ciks (symbol, cik, company_name, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![symbol, cik, company_name, updated_at],
        )?;

        Ok(())
    }

    /// Get all financial statements for a symbol.
    pub fn get_financial_statements(&self, symbol: &str) -> Result<Vec<FinancialStatement>> {
        let mut stmt = self.conn.prepare(
            "SELECT symbol, cik, period_end, period_type, fiscal_year, fiscal_quarter,
                    total_assets, total_liabilities, stockholders_equity, long_term_debt,
                    current_assets, current_liabilities, cash_and_equivalents,
                    revenue, net_income, operating_income, gross_profit, eps_basic, eps_diluted,
                    operating_cash_flow, capital_expenditures, free_cash_flow,
                    shares_outstanding, shares_outstanding_diluted, cached_at
             FROM financial_statements
             WHERE symbol = ?1
             ORDER BY period_end DESC",
        )?;

        let rows = stmt.query_map(params![symbol], |row| {
            Ok(FinancialStatement {
                symbol: row.get(0)?,
                cik: row.get(1)?,
                period_end: NaiveDate::parse_from_str(&row.get::<_, String>(2)?, "%Y-%m-%d")
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                period_type: PeriodType::from_db_str(&row.get::<_, String>(3)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                fiscal_year: row.get(4)?,
                fiscal_quarter: row.get(5)?,
                total_assets: row.get(6)?,
                total_liabilities: row.get(7)?,
                stockholders_equity: row.get(8)?,
                long_term_debt: row.get(9)?,
                current_assets: row.get(10)?,
                current_liabilities: row.get(11)?,
                cash_and_equivalents: row.get(12)?,
                revenue: row.get(13)?,
                net_income: row.get(14)?,
                operating_income: row.get(15)?,
                gross_profit: row.get(16)?,
                eps_basic: row.get(17)?,
                eps_diluted: row.get(18)?,
                operating_cash_flow: row.get(19)?,
                capital_expenditures: row.get(20)?,
                free_cash_flow: row.get(21)?,
                shares_outstanding: row.get(22)?,
                shares_outstanding_diluted: row.get(23)?,
                cached_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(24)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&Utc),
            })
        })?;

        let mut statements = Vec::new();
        for row in rows {
            statements.push(row?);
        }

        Ok(statements)
    }

    /// Get the latest financial statement for a symbol and period type.
    pub fn get_latest_financial(
        &self,
        symbol: &str,
        period_type: PeriodType,
    ) -> Result<Option<FinancialStatement>> {
        let result = self
            .conn
            .query_row(
                "SELECT symbol, cik, period_end, period_type, fiscal_year, fiscal_quarter,
                    total_assets, total_liabilities, stockholders_equity, long_term_debt,
                    current_assets, current_liabilities, cash_and_equivalents,
                    revenue, net_income, operating_income, gross_profit, eps_basic, eps_diluted,
                    operating_cash_flow, capital_expenditures, free_cash_flow,
                    shares_outstanding, shares_outstanding_diluted, cached_at
             FROM financial_statements
             WHERE symbol = ?1 AND period_type = ?2
             ORDER BY period_end DESC
             LIMIT 1",
                params![symbol, period_type.to_db_str()],
                |row| {
                    Ok(FinancialStatement {
                        symbol: row.get(0)?,
                        cik: row.get(1)?,
                        period_end: NaiveDate::parse_from_str(
                            &row.get::<_, String>(2)?,
                            "%Y-%m-%d",
                        )
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                        period_type: PeriodType::from_db_str(&row.get::<_, String>(3)?)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                        fiscal_year: row.get(4)?,
                        fiscal_quarter: row.get(5)?,
                        total_assets: row.get(6)?,
                        total_liabilities: row.get(7)?,
                        stockholders_equity: row.get(8)?,
                        long_term_debt: row.get(9)?,
                        current_assets: row.get(10)?,
                        current_liabilities: row.get(11)?,
                        cash_and_equivalents: row.get(12)?,
                        revenue: row.get(13)?,
                        net_income: row.get(14)?,
                        operating_income: row.get(15)?,
                        gross_profit: row.get(16)?,
                        eps_basic: row.get(17)?,
                        eps_diluted: row.get(18)?,
                        operating_cash_flow: row.get(19)?,
                        capital_expenditures: row.get(20)?,
                        free_cash_flow: row.get(21)?,
                        shares_outstanding: row.get(22)?,
                        shares_outstanding_diluted: row.get(23)?,
                        cached_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(24)?)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                            .with_timezone(&Utc),
                    })
                },
            )
            .optional()?;

        Ok(result)
    }

    /// Store a single financial statement.
    pub fn put_financial_statement(&self, stmt: &FinancialStatement) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO financial_statements (
                symbol, cik, period_end, period_type, fiscal_year, fiscal_quarter,
                total_assets, total_liabilities, stockholders_equity, long_term_debt,
                current_assets, current_liabilities, cash_and_equivalents,
                revenue, net_income, operating_income, gross_profit, eps_basic, eps_diluted,
                operating_cash_flow, capital_expenditures, free_cash_flow,
                shares_outstanding, shares_outstanding_diluted, cached_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)",
            params![
                stmt.symbol,
                stmt.cik,
                stmt.period_end.to_string(),
                stmt.period_type.to_db_str(),
                stmt.fiscal_year,
                stmt.fiscal_quarter,
                stmt.total_assets,
                stmt.total_liabilities,
                stmt.stockholders_equity,
                stmt.long_term_debt,
                stmt.current_assets,
                stmt.current_liabilities,
                stmt.cash_and_equivalents,
                stmt.revenue,
                stmt.net_income,
                stmt.operating_income,
                stmt.gross_profit,
                stmt.eps_basic,
                stmt.eps_diluted,
                stmt.operating_cash_flow,
                stmt.capital_expenditures,
                stmt.free_cash_flow,
                stmt.shares_outstanding,
                stmt.shares_outstanding_diluted,
                stmt.cached_at.to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    /// Store multiple financial statements in a batch.
    pub fn put_financial_statements_batch(&self, stmts: &[FinancialStatement]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        for stmt in stmts {
            tx.execute(
                "INSERT OR REPLACE INTO financial_statements (
                    symbol, cik, period_end, period_type, fiscal_year, fiscal_quarter,
                    total_assets, total_liabilities, stockholders_equity, long_term_debt,
                    current_assets, current_liabilities, cash_and_equivalents,
                    revenue, net_income, operating_income, gross_profit, eps_basic, eps_diluted,
                    operating_cash_flow, capital_expenditures, free_cash_flow,
                    shares_outstanding, shares_outstanding_diluted, cached_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)",
                params![
                    stmt.symbol,
                    stmt.cik,
                    stmt.period_end.to_string(),
                    stmt.period_type.to_db_str(),
                    stmt.fiscal_year,
                    stmt.fiscal_quarter,
                    stmt.total_assets,
                    stmt.total_liabilities,
                    stmt.stockholders_equity,
                    stmt.long_term_debt,
                    stmt.current_assets,
                    stmt.current_liabilities,
                    stmt.cash_and_equivalents,
                    stmt.revenue,
                    stmt.net_income,
                    stmt.operating_income,
                    stmt.gross_profit,
                    stmt.eps_basic,
                    stmt.eps_diluted,
                    stmt.operating_cash_flow,
                    stmt.capital_expenditures,
                    stmt.free_cash_flow,
                    stmt.shares_outstanding,
                    stmt.shares_outstanding_diluted,
                    stmt.cached_at.to_rfc3339(),
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// Check if we have recent financial statements for a symbol.
    pub fn has_recent_financials(&self, symbol: &str, max_age_days: i64) -> Result<bool> {
        let cutoff = Utc::now() - chrono::Duration::days(max_age_days);
        let cutoff_str = cutoff.to_rfc3339();

        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM financial_statements
             WHERE symbol = ?1 AND cached_at >= ?2",
            params![symbol, cutoff_str],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }

    /// Clear all cached data.
    pub fn clear_all(&self) -> Result<()> {
        self.conn.execute("DELETE FROM quotes", [])?;
        self.conn.execute("DELETE FROM market_caps", [])?;
        self.conn.execute("DELETE FROM fundamentals", [])?;
        self.conn.execute("DELETE FROM financial_statements", [])?;
        self.conn.execute("DELETE FROM company_ciks", [])?;
        Ok(())
    }

    /// Clear cached data for a specific symbol.
    pub fn clear_symbol(&self, symbol: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM quotes WHERE symbol = ?1", params![symbol])?;
        self.conn
            .execute("DELETE FROM market_caps WHERE symbol = ?1", params![symbol])?;
        self.conn.execute(
            "DELETE FROM fundamentals WHERE symbol = ?1",
            params![symbol],
        )?;
        self.conn.execute(
            "DELETE FROM financial_statements WHERE symbol = ?1",
            params![symbol],
        )?;
        self.conn.execute(
            "DELETE FROM company_ciks WHERE symbol = ?1",
            params![symbol],
        )?;
        Ok(())
    }

    /// Get cache statistics.
    pub fn get_stats(&self) -> Result<CacheStats> {
        let quotes_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM quotes", [], |row| row.get(0))?;

        let symbols_count: i64 =
            self.conn
                .query_row("SELECT COUNT(DISTINCT symbol) FROM quotes", [], |row| {
                    row.get(0)
                })?;

        let universe_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM universe WHERE active = 1",
            [],
            |row| row.get(0),
        )?;

        let financial_statements_count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM financial_statements", [], |row| {
                    row.get(0)
                })?;

        let cik_mappings_count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM company_ciks", [], |row| row.get(0))?;

        Ok(CacheStats {
            total_quotes: quotes_count as usize,
            unique_symbols: symbols_count as usize,
            universe_size: universe_count as usize,
            financial_statements: financial_statements_count as usize,
            cik_mappings: cik_mappings_count as usize,
        })
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Total number of quote records
    pub total_quotes: usize,
    /// Number of unique symbols
    pub unique_symbols: usize,
    /// Size of the universe
    pub universe_size: usize,
    /// Number of financial statements cached
    pub financial_statements: usize,
    /// Number of CIK mappings
    pub cik_mappings: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_cache_initialization() {
        let cache = SqliteCache::in_memory();
        assert!(cache.is_ok());
    }

    #[test]
    fn test_universe_operations() {
        let cache = SqliteCache::in_memory().unwrap();

        // Add symbols
        cache
            .add_to_universe("AAPL", Some("Apple Inc."), Some("Technology"), None)
            .unwrap();
        cache
            .add_to_universe("MSFT", Some("Microsoft"), Some("Technology"), None)
            .unwrap();

        // Get universe
        let universe = cache.get_universe().unwrap();
        assert_eq!(universe.len(), 2);
        assert!(universe.contains(&"AAPL".to_string()));
        assert!(universe.contains(&"MSFT".to_string()));

        // Remove symbol
        cache.remove_from_universe("AAPL").unwrap();
        let universe = cache.get_universe().unwrap();
        assert_eq!(universe.len(), 1);
        assert!(!universe.contains(&"AAPL".to_string()));
    }

    #[test]
    fn test_market_cap_operations() {
        let cache = SqliteCache::in_memory().unwrap();
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        // Put market cap
        cache
            .put_market_cap("AAPL", date, 3_000_000_000_000.0)
            .unwrap();

        // Get market cap
        let market_cap = cache.get_market_cap("AAPL", date).unwrap();
        assert_eq!(market_cap, Some(3_000_000_000_000.0));

        // Get non-existent market cap
        let market_cap = cache.get_market_cap("MSFT", date).unwrap();
        assert_eq!(market_cap, None);
    }

    #[test]
    fn test_cache_stats() {
        let cache = SqliteCache::in_memory().unwrap();

        let stats = cache.get_stats().unwrap();
        assert_eq!(stats.total_quotes, 0);
        assert_eq!(stats.unique_symbols, 0);
        assert_eq!(stats.universe_size, 0);
        assert_eq!(stats.financial_statements, 0);
        assert_eq!(stats.cik_mappings, 0);
    }

    #[test]
    fn test_cik_operations() {
        let cache = SqliteCache::in_memory().unwrap();

        // Put CIK
        cache
            .put_cik("AAPL", "0000320193", Some("Apple Inc."))
            .unwrap();

        // Get CIK
        let cik = cache.get_cik("AAPL").unwrap();
        assert_eq!(cik, Some("0000320193".to_string()));

        // Get non-existent CIK
        let cik = cache.get_cik("MSFT").unwrap();
        assert_eq!(cik, None);

        // Update CIK
        cache
            .put_cik("AAPL", "0000320193", Some("Apple Inc. Updated"))
            .unwrap();
        let cik = cache.get_cik("AAPL").unwrap();
        assert_eq!(cik, Some("0000320193".to_string()));
    }

    #[test]
    fn test_financial_statement_operations() {
        let cache = SqliteCache::in_memory().unwrap();

        let stmt = FinancialStatement {
            symbol: "AAPL".to_string(),
            cik: "0000320193".to_string(),
            period_end: NaiveDate::from_ymd_opt(2024, 9, 30).unwrap(),
            period_type: PeriodType::Quarterly,
            fiscal_year: 2024,
            fiscal_quarter: Some(4),
            total_assets: Some(365_725_000_000.0),
            total_liabilities: Some(308_030_000_000.0),
            stockholders_equity: Some(57_695_000_000.0),
            long_term_debt: Some(97_000_000_000.0),
            current_assets: Some(143_566_000_000.0),
            current_liabilities: Some(157_308_000_000.0),
            cash_and_equivalents: Some(30_000_000_000.0),
            revenue: Some(94_930_000_000.0),
            net_income: Some(14_736_000_000.0),
            operating_income: Some(23_854_000_000.0),
            gross_profit: Some(43_880_000_000.0),
            eps_basic: Some(0.97),
            eps_diluted: Some(0.97),
            operating_cash_flow: Some(26_000_000_000.0),
            capital_expenditures: Some(2_500_000_000.0),
            free_cash_flow: Some(23_500_000_000.0),
            shares_outstanding: Some(15_200_000_000.0),
            shares_outstanding_diluted: Some(15_200_000_000.0),
            cached_at: Utc::now(),
        };

        // Put statement
        cache.put_financial_statement(&stmt).unwrap();

        // Get all statements
        let statements = cache.get_financial_statements("AAPL").unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].symbol, "AAPL");
        assert_eq!(statements[0].fiscal_year, 2024);
        assert_eq!(statements[0].period_type, PeriodType::Quarterly);

        // Get latest quarterly
        let latest = cache
            .get_latest_financial("AAPL", PeriodType::Quarterly)
            .unwrap();
        assert!(latest.is_some());
        let latest = latest.unwrap();
        assert_eq!(latest.fiscal_year, 2024);
        assert_eq!(latest.fiscal_quarter, Some(4));

        // Get latest annual (should be None)
        let latest = cache
            .get_latest_financial("AAPL", PeriodType::Annual)
            .unwrap();
        assert!(latest.is_none());

        // Check stats
        let stats = cache.get_stats().unwrap();
        assert_eq!(stats.financial_statements, 1);
    }

    #[test]
    fn test_financial_statement_batch_operations() {
        let cache = SqliteCache::in_memory().unwrap();

        let stmts = vec![
            FinancialStatement {
                symbol: "AAPL".to_string(),
                cik: "0000320193".to_string(),
                period_end: NaiveDate::from_ymd_opt(2024, 9, 30).unwrap(),
                period_type: PeriodType::Quarterly,
                fiscal_year: 2024,
                fiscal_quarter: Some(4),
                total_assets: Some(365_725_000_000.0),
                total_liabilities: Some(308_030_000_000.0),
                stockholders_equity: Some(57_695_000_000.0),
                long_term_debt: None,
                current_assets: None,
                current_liabilities: None,
                cash_and_equivalents: None,
                revenue: Some(94_930_000_000.0),
                net_income: Some(14_736_000_000.0),
                operating_income: None,
                gross_profit: None,
                eps_basic: None,
                eps_diluted: None,
                operating_cash_flow: None,
                capital_expenditures: None,
                free_cash_flow: None,
                shares_outstanding: None,
                shares_outstanding_diluted: None,
                cached_at: Utc::now(),
            },
            FinancialStatement {
                symbol: "AAPL".to_string(),
                cik: "0000320193".to_string(),
                period_end: NaiveDate::from_ymd_opt(2024, 6, 30).unwrap(),
                period_type: PeriodType::Quarterly,
                fiscal_year: 2024,
                fiscal_quarter: Some(3),
                total_assets: Some(353_000_000_000.0),
                total_liabilities: Some(296_000_000_000.0),
                stockholders_equity: Some(57_000_000_000.0),
                long_term_debt: None,
                current_assets: None,
                current_liabilities: None,
                cash_and_equivalents: None,
                revenue: Some(85_777_000_000.0),
                net_income: Some(21_448_000_000.0),
                operating_income: None,
                gross_profit: None,
                eps_basic: None,
                eps_diluted: None,
                operating_cash_flow: None,
                capital_expenditures: None,
                free_cash_flow: None,
                shares_outstanding: None,
                shares_outstanding_diluted: None,
                cached_at: Utc::now(),
            },
            FinancialStatement {
                symbol: "AAPL".to_string(),
                cik: "0000320193".to_string(),
                period_end: NaiveDate::from_ymd_opt(2023, 9, 30).unwrap(),
                period_type: PeriodType::Annual,
                fiscal_year: 2023,
                fiscal_quarter: None,
                total_assets: Some(352_755_000_000.0),
                total_liabilities: Some(290_437_000_000.0),
                stockholders_equity: Some(62_318_000_000.0),
                long_term_debt: None,
                current_assets: None,
                current_liabilities: None,
                cash_and_equivalents: None,
                revenue: Some(383_285_000_000.0),
                net_income: Some(96_995_000_000.0),
                operating_income: None,
                gross_profit: None,
                eps_basic: None,
                eps_diluted: None,
                operating_cash_flow: None,
                capital_expenditures: None,
                free_cash_flow: None,
                shares_outstanding: None,
                shares_outstanding_diluted: None,
                cached_at: Utc::now(),
            },
        ];

        // Batch insert
        cache.put_financial_statements_batch(&stmts).unwrap();

        // Verify all were inserted
        let statements = cache.get_financial_statements("AAPL").unwrap();
        assert_eq!(statements.len(), 3);

        // Verify sorting (most recent first)
        assert_eq!(
            statements[0].period_end,
            NaiveDate::from_ymd_opt(2024, 9, 30).unwrap()
        );
        assert_eq!(
            statements[1].period_end,
            NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()
        );
        assert_eq!(
            statements[2].period_end,
            NaiveDate::from_ymd_opt(2023, 9, 30).unwrap()
        );

        // Get latest quarterly
        let latest = cache
            .get_latest_financial("AAPL", PeriodType::Quarterly)
            .unwrap()
            .unwrap();
        assert_eq!(
            latest.period_end,
            NaiveDate::from_ymd_opt(2024, 9, 30).unwrap()
        );

        // Get latest annual
        let latest = cache
            .get_latest_financial("AAPL", PeriodType::Annual)
            .unwrap()
            .unwrap();
        assert_eq!(
            latest.period_end,
            NaiveDate::from_ymd_opt(2023, 9, 30).unwrap()
        );
    }

    #[test]
    fn test_has_recent_financials() {
        let cache = SqliteCache::in_memory().unwrap();

        // No recent data
        assert!(!cache.has_recent_financials("AAPL", 90).unwrap());

        // Add a statement
        let stmt = FinancialStatement {
            symbol: "AAPL".to_string(),
            cik: "0000320193".to_string(),
            period_end: NaiveDate::from_ymd_opt(2024, 9, 30).unwrap(),
            period_type: PeriodType::Quarterly,
            fiscal_year: 2024,
            fiscal_quarter: Some(4),
            total_assets: None,
            total_liabilities: None,
            stockholders_equity: None,
            long_term_debt: None,
            current_assets: None,
            current_liabilities: None,
            cash_and_equivalents: None,
            revenue: None,
            net_income: None,
            operating_income: None,
            gross_profit: None,
            eps_basic: None,
            eps_diluted: None,
            operating_cash_flow: None,
            capital_expenditures: None,
            free_cash_flow: None,
            shares_outstanding: None,
            shares_outstanding_diluted: None,
            cached_at: Utc::now(),
        };

        cache.put_financial_statement(&stmt).unwrap();

        // Should have recent data
        assert!(cache.has_recent_financials("AAPL", 90).unwrap());

        // Should not have data older than 0 days
        assert!(!cache.has_recent_financials("AAPL", 0).unwrap());
    }

    #[test]
    fn test_clear_operations_with_edgar() {
        let cache = SqliteCache::in_memory().unwrap();

        // Add CIK and financial statement
        cache
            .put_cik("AAPL", "0000320193", Some("Apple Inc."))
            .unwrap();

        let stmt = FinancialStatement {
            symbol: "AAPL".to_string(),
            cik: "0000320193".to_string(),
            period_end: NaiveDate::from_ymd_opt(2024, 9, 30).unwrap(),
            period_type: PeriodType::Quarterly,
            fiscal_year: 2024,
            fiscal_quarter: Some(4),
            total_assets: None,
            total_liabilities: None,
            stockholders_equity: None,
            long_term_debt: None,
            current_assets: None,
            current_liabilities: None,
            cash_and_equivalents: None,
            revenue: None,
            net_income: None,
            operating_income: None,
            gross_profit: None,
            eps_basic: None,
            eps_diluted: None,
            operating_cash_flow: None,
            capital_expenditures: None,
            free_cash_flow: None,
            shares_outstanding: None,
            shares_outstanding_diluted: None,
            cached_at: Utc::now(),
        };

        cache.put_financial_statement(&stmt).unwrap();

        // Verify data exists
        assert!(cache.get_cik("AAPL").unwrap().is_some());
        assert!(!cache.get_financial_statements("AAPL").unwrap().is_empty());

        // Clear symbol
        cache.clear_symbol("AAPL").unwrap();

        // Verify data is cleared
        assert!(cache.get_cik("AAPL").unwrap().is_none());
        assert!(cache.get_financial_statements("AAPL").unwrap().is_empty());
    }

    #[test]
    fn test_period_type_conversion() {
        assert_eq!(PeriodType::Quarterly.to_db_str(), "Q");
        assert_eq!(PeriodType::Annual.to_db_str(), "A");

        assert_eq!(PeriodType::from_db_str("Q").unwrap(), PeriodType::Quarterly);
        assert_eq!(PeriodType::from_db_str("A").unwrap(), PeriodType::Annual);
        assert!(PeriodType::from_db_str("X").is_err());
    }
}
