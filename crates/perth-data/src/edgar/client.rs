//! SEC EDGAR API client with rate limiting.

use crate::error::{DataError, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

/// SEC EDGAR API base URL
const EDGAR_BASE_URL: &str = "https://data.sec.gov";

/// Default rate limit: 10 requests per second (SEC requirement)
const DEFAULT_RATE_LIMIT: Duration = Duration::from_millis(100);

/// User agent for SEC EDGAR requests (SEC requires identifying information)
const USER_AGENT: &str = "Perth-FactorModel/0.1 (contact@example.com)";

/// Company information from tickers endpoint
/// The SEC returns: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CompanyInfo {
    /// CIK as a number (SEC returns this as an integer despite the name)
    cik_str: u64,
    /// Ticker symbol
    ticker: String,
    /// Company name
    title: String,
}

/// Company filings metadata from SEC submissions API
#[derive(Debug, Clone, Deserialize)]
pub struct CompanyFilings {
    /// CIK number
    pub cik: String,
    /// Company name
    pub name: String,
    /// Filing history container
    pub filings: FilingsContainer,
}

/// Container for filings data
#[derive(Debug, Clone, Deserialize)]
pub struct FilingsContainer {
    /// Recent filings
    pub recent: FilingsRecent,
}

/// Recent filings data
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilingsRecent {
    /// Accession numbers
    pub accession_number: Vec<String>,
    /// Filing dates
    pub filing_date: Vec<String>,
    /// Report dates
    #[serde(default)]
    pub report_date: Vec<String>,
    /// Form types (e.g., "10-K", "10-Q")
    pub form: Vec<String>,
    /// Primary documents
    pub primary_document: Vec<String>,
}

/// Rate limiter to ensure we don't exceed SEC's rate limits
struct RateLimiter {
    last_request: Instant,
    min_interval: Duration,
}

impl RateLimiter {
    fn new(min_interval: Duration) -> Self {
        Self {
            last_request: Instant::now() - min_interval,
            min_interval,
        }
    }

    async fn wait(&mut self) {
        let elapsed = self.last_request.elapsed();
        if elapsed < self.min_interval {
            sleep(self.min_interval - elapsed).await;
        }
        self.last_request = Instant::now();
    }
}

/// SEC EDGAR API client with rate limiting
pub struct EdgarClient {
    client: reqwest::Client,
    rate_limiter: Arc<Mutex<RateLimiter>>,
    base_url: String,
}

impl EdgarClient {
    /// Create a new EDGAR client with default settings (10 req/sec)
    pub fn new() -> Result<Self> {
        Self::with_rate_limit(DEFAULT_RATE_LIMIT)
    }

    /// Create a new EDGAR client with custom rate limit
    ///
    /// # Arguments
    /// * `min_interval` - Minimum duration between requests
    ///
    /// # Example
    /// ```no_run
    /// use perth_data::edgar::EdgarClient;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> perth_data::Result<()> {
    /// // 5 requests per second
    /// let client = EdgarClient::with_rate_limit(Duration::from_millis(200))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_rate_limit(min_interval: Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(DataError::Network)?;

        Ok(Self {
            client,
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new(min_interval))),
            base_url: EDGAR_BASE_URL.to_string(),
        })
    }

    /// Look up a company's CIK number from its ticker symbol
    ///
    /// # Arguments
    /// * `ticker` - Stock ticker symbol (e.g., "AAPL")
    ///
    /// # Returns
    /// The company's CIK number as a zero-padded 10-digit string
    ///
    /// # Errors
    /// Returns `DataError::CikNotFound` if the ticker is not found
    ///
    /// # Example
    /// ```no_run
    /// use perth_data::edgar::EdgarClient;
    ///
    /// # async fn example() -> perth_data::Result<()> {
    /// let client = EdgarClient::new()?;
    /// let cik = client.get_company_cik("AAPL").await?;
    /// println!("Apple CIK: {}", cik);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_company_cik(&self, ticker: &str) -> Result<String> {
        // Validate ticker
        if ticker.is_empty() {
            return Err(DataError::InvalidSymbol("Empty ticker".to_string()));
        }

        let ticker_upper = ticker.to_uppercase();

        // Rate limit
        self.rate_limiter.lock().await.wait().await;

        // Fetch company tickers JSON (note: hosted at www.sec.gov, not data.sec.gov)
        let url = "https://www.sec.gov/files/company_tickers.json".to_string();
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(DataError::Network)?;

        if !response.status().is_success() {
            return Err(DataError::EdgarApi(format!(
                "Failed to fetch company tickers: HTTP {}",
                response.status()
            )));
        }

        // Parse as a map of index -> CompanyInfo
        let data: HashMap<String, CompanyInfo> = response
            .json()
            .await
            .map_err(|e| DataError::EdgarApi(format!("Failed to parse company tickers: {}", e)))?;

        // Search for ticker in the response
        for company in data.values() {
            if company.ticker.to_uppercase() == ticker_upper {
                // CIK should be zero-padded to 10 digits
                let cik = format!("{:0>10}", company.cik_str);
                return Ok(cik);
            }
        }

        Err(DataError::CikNotFound(ticker.to_string()))
    }

    /// Get company filings metadata
    ///
    /// # Arguments
    /// * `cik` - Company's CIK number (can be with or without padding)
    ///
    /// # Returns
    /// Company filings metadata including recent filings
    ///
    /// # Example
    /// ```no_run
    /// use perth_data::edgar::EdgarClient;
    ///
    /// # async fn example() -> perth_data::Result<()> {
    /// let client = EdgarClient::new()?;
    /// let cik = client.get_company_cik("AAPL").await?;
    /// let filings = client.get_company_filings(&cik).await?;
    /// println!("Found {} recent filings", filings.filings.recent.accession_number.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_company_filings(&self, cik: &str) -> Result<CompanyFilings> {
        // Validate CIK
        if cik.is_empty() {
            return Err(DataError::InvalidSymbol("Empty CIK".to_string()));
        }

        // Ensure CIK is zero-padded to 10 digits
        let cik_padded = format!("{:0>10}", cik);

        // Rate limit
        self.rate_limiter.lock().await.wait().await;

        // Fetch company filings JSON
        let url = format!("{}/submissions/CIK{}.json", self.base_url, cik_padded);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(DataError::Network)?;

        if !response.status().is_success() {
            return Err(DataError::EdgarApi(format!(
                "Failed to fetch company filings for CIK {}: HTTP {}",
                cik_padded,
                response.status()
            )));
        }

        let filings: CompanyFilings = response
            .json()
            .await
            .map_err(|e| DataError::EdgarApi(format!("Failed to parse company filings: {}", e)))?;

        Ok(filings)
    }

    /// Fetch a raw filing document
    ///
    /// # Arguments
    /// * `cik` - Company's CIK number
    /// * `accession` - Accession number (e.g., "0000320193-23-000077")
    /// * `document` - Document filename (e.g., "aapl-20230930.htm")
    ///
    /// # Returns
    /// The raw document content as a string
    ///
    /// # Example
    /// ```no_run
    /// use perth_data::edgar::EdgarClient;
    ///
    /// # async fn example() -> perth_data::Result<()> {
    /// let client = EdgarClient::new()?;
    /// let cik = client.get_company_cik("AAPL").await?;
    /// let filings = client.get_company_filings(&cik).await?;
    ///
    /// if let Some(accession) = filings.filings.recent.accession_number.first() {
    ///     if let Some(doc) = filings.filings.recent.primary_document.first() {
    ///         let content = client.get_filing_document(&cik, accession, doc).await?;
    ///         println!("Document size: {} bytes", content.len());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_filing_document(
        &self,
        cik: &str,
        accession: &str,
        document: &str,
    ) -> Result<String> {
        // Validate inputs
        if cik.is_empty() {
            return Err(DataError::InvalidSymbol("Empty CIK".to_string()));
        }
        if accession.is_empty() {
            return Err(DataError::EdgarApi("Empty accession number".to_string()));
        }
        if document.is_empty() {
            return Err(DataError::EdgarApi("Empty document name".to_string()));
        }

        // Ensure CIK is zero-padded to 10 digits
        let cik_padded = format!("{:0>10}", cik);

        // Remove dashes from accession number for URL
        let accession_no_dash = accession.replace('-', "");

        // Rate limit
        self.rate_limiter.lock().await.wait().await;

        // Construct document URL
        let url = format!(
            "{}/Archives/edgar/data/{}/{}/{}",
            self.base_url, cik_padded, accession_no_dash, document
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(DataError::Network)?;

        if !response.status().is_success() {
            return Err(DataError::EdgarApi(format!(
                "Failed to fetch filing document: HTTP {}",
                response.status()
            )));
        }

        let content = response
            .text()
            .await
            .map_err(|e| DataError::EdgarApi(format!("Failed to read document content: {}", e)))?;

        Ok(content)
    }
}

impl Default for EdgarClient {
    fn default() -> Self {
        Self::new().expect("Failed to create EDGAR client")
    }
}

impl std::fmt::Debug for EdgarClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgarClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_company_cik() {
        let client = EdgarClient::new().unwrap();
        let result = client.get_company_cik("AAPL").await;
        assert!(result.is_ok());
        let cik = result.unwrap();
        // Apple's CIK should be 10 digits
        assert_eq!(cik.len(), 10);
        assert!(cik.parse::<u64>().is_ok());
    }

    #[tokio::test]
    async fn test_get_company_cik_lowercase() {
        let client = EdgarClient::new().unwrap();
        let result = client.get_company_cik("aapl").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_company_cik_not_found() {
        let client = EdgarClient::new().unwrap();
        let result = client.get_company_cik("NOTAREALTICKER123").await;
        assert!(matches!(result, Err(DataError::CikNotFound(_))));
    }

    #[tokio::test]
    async fn test_get_company_cik_empty() {
        let client = EdgarClient::new().unwrap();
        let result = client.get_company_cik("").await;
        assert!(matches!(result, Err(DataError::InvalidSymbol(_))));
    }

    #[tokio::test]
    async fn test_get_company_filings() {
        let client = EdgarClient::new().unwrap();
        let cik = client.get_company_cik("AAPL").await.unwrap();
        let result = client.get_company_filings(&cik).await;
        assert!(result.is_ok(), "Failed to get filings: {:?}", result.err());

        let filings = result.unwrap();
        assert!(!filings.filings.recent.accession_number.is_empty());
        assert_eq!(
            filings.filings.recent.accession_number.len(),
            filings.filings.recent.filing_date.len()
        );
        assert_eq!(
            filings.filings.recent.accession_number.len(),
            filings.filings.recent.form.len()
        );
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let client = EdgarClient::with_rate_limit(Duration::from_millis(200)).unwrap();

        let start = Instant::now();

        // Make 3 requests
        let _ = client.get_company_cik("AAPL").await;
        let _ = client.get_company_cik("MSFT").await;
        let _ = client.get_company_cik("GOOGL").await;

        let elapsed = start.elapsed();

        // Should take at least 400ms (2 intervals between 3 requests)
        assert!(elapsed >= Duration::from_millis(400));
    }

    #[tokio::test]
    async fn test_custom_rate_limit() {
        let _client = EdgarClient::with_rate_limit(Duration::from_millis(50)).unwrap();
        // Client created successfully with custom rate limit
    }
}
