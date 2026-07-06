# crypto-etl-poc

An end-to-end ETL pipeline that extracts cryptocurrency market data from public APIs, stores the raw responses, transforms the data into analytical layers, and exposes the results through a visual dashboard.

## Overview

`crypto-etl-poc` is a Data Engineering proof of concept focused on building a complete local-first analytics pipeline for cryptocurrency data.

The project follows a layered architecture:

```text
Crypto API
  → Raw JSON storage
  → Bronze normalized data
  → Silver cleaned data
  → Gold analytical tables
  → Dashboard
```

The goal is to demonstrate a professional ETL workflow, including extraction, raw data preservation, transformation, data quality checks, logging, and visualization.

## Development Approach

This implementation will be developed using a mixed approach that combines AI-assisted support with manual programming. AI may be used to accelerate planning, boilerplate generation, refactoring suggestions, documentation, testing ideas, and debugging support. Manual programming will be used to review, validate, adapt, and implement the final code to ensure correctness, maintainability, and a clear understanding of the solution.

The project should not rely on generated code blindly. Every important decision, transformation rule, data model, and pipeline behavior should be reviewed and adjusted manually before being considered complete.

## Main Goals

This project should allow users to analyze cryptocurrency market behavior over time and answer questions such as:

- What is the current price of selected cryptocurrencies?
- How have prices changed over the last 24 hours, 7 days, and 30 days?
- Which coins have the highest market capitalization?
- Which coins have the highest daily volume?
- Which coins are the best and worst performers?
- Which coins are the most volatile?
- How does the total market capitalization evolve over time?

## MVP Scope

The first version should include:

- Configurable list of cryptocurrencies.
- Extraction of market data from a public cryptocurrency API.
- Storage of raw API responses without modification.
- Transformation into structured analytical datasets.
- Data quality checks.
- Local analytical storage using Parquet and/or DuckDB.
- A simple dashboard for visual exploration.
- Logging and error handling.
- Basic test coverage.
- Clear documentation.

## Recommended Data Source

The recommended primary data source for the MVP is the CoinGecko API.

Suggested data to extract:

- Current prices.
- Market capitalization.
- Trading volume.
- 24-hour high and low prices.
- 24-hour price change.
- Historical price data.
- Coin metadata.

An optional secondary source is Binance public market data, especially for OHLCV candlestick data.

## Example Cryptocurrencies

The initial version can track a small configurable list of coins:

```yaml
coins:
  - bitcoin
  - ethereum
  - solana
  - binancecoin
  - ripple
  - cardano
  - dogecoin
currency: eur
historical_days: 90
```

The coin list should be stored in a configuration file, not hardcoded inside the pipeline logic.

## Suggested Tech Stack

Recommended local-first stack:

- Python 3.11+
- requests or httpx
- pandas or polars
- pydantic
- DuckDB
- Parquet
- Streamlit
- pytest
- Docker
- ruff or black

Optional future additions:

- dbt
- Prefect, Airflow, or Dagster
- PostgreSQL
- Power BI
- Azure Data Lake Storage
- Snowflake
- Microsoft Fabric
- GitHub Actions

## Project Structure

```text
crypto-etl-poc/
│
├── README.md
├── pyproject.toml
├── .env.example
├── docker-compose.yml
├── config/
│   └── config.yaml
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   └── crypto_etl/
│       ├── __init__.py
│       ├── config.py
│       ├── logging_config.py
│       ├── clients/
│       │   └── coingecko_client.py
│       ├── extract/
│       │   └── extract_market_data.py
│       ├── load/
│       │   └── raw_loader.py
│       ├── transform/
│       │   ├── bronze_transform.py
│       │   ├── silver_transform.py
│       │   └── gold_transform.py
│       ├── quality/
│       │   └── checks.py
│       ├── orchestration/
│       │   └── run_pipeline.py
│       └── utils/
│           └── time.py
│
├── dashboard/
│   └── app.py
│
├── tests/
│   ├── test_config.py
│   ├── test_transformations.py
│   └── test_quality_checks.py
│
└── notebooks/
    └── exploratory_analysis.ipynb
```

## Data Architecture

### Raw Layer

The raw layer stores the original API responses exactly as received.

Purpose:

- Preserve the source response.
- Allow future reprocessing.
- Support debugging.
- Keep an audit trail.

Example path:

```text
data/raw/coingecko/market_data/year=2026/month=07/day=06/run_id=<run_id>/response.json
```

Example raw object:

```json
{
  "provider": "coingecko",
  "endpoint": "coins_markets",
  "requested_at_utc": "2026-07-06T10:30:00Z",
  "params": {
    "vs_currency": "eur",
    "ids": ["bitcoin", "ethereum", "solana"]
  },
  "response": []
}
```

### Bronze Layer

The bronze layer contains normalized data extracted from the raw JSON files.

Example table:

```text
bronze_crypto_market_snapshot
```

Suggested columns:

```text
run_id
provider
extracted_at_utc
coin_id
symbol
name
current_price
market_cap
market_cap_rank
fully_diluted_valuation
total_volume
high_24h
low_24h
price_change_24h
price_change_percentage_24h
market_cap_change_24h
market_cap_change_percentage_24h
circulating_supply
total_supply
max_supply
ath
ath_change_percentage
ath_date
atl
atl_change_percentage
atl_date
last_updated
raw_file_path
```

### Silver Layer

The silver layer contains cleaned, typed, deduplicated, and standardized data.

Example table:

```text
silver_crypto_market_snapshot
```

Cleaning rules:

- Convert timestamps to UTC.
- Cast numeric fields correctly.
- Remove duplicate rows.
- Standardize coin symbols to uppercase.
- Validate prices, market cap, and volume.
- Keep one clean row per coin per extraction timestamp.

Suggested columns:

```text
snapshot_id
coin_id
symbol
name
currency
price
market_cap
market_cap_rank
volume_24h
high_24h
low_24h
price_change_24h
price_change_pct_24h
market_cap_change_24h
market_cap_change_pct_24h
circulating_supply
total_supply
max_supply
ath
ath_date
atl
atl_date
last_updated_utc
extracted_at_utc
provider
```

### Gold Layer

The gold layer contains business-friendly analytical tables ready for dashboards and reporting.

Recommended gold tables:

#### gold_crypto_latest_prices

Latest available price per coin.

```text
coin_id
symbol
name
currency
latest_price
market_cap
market_cap_rank
volume_24h
price_change_pct_24h
last_updated_utc
```

#### gold_crypto_performance

Performance metrics by coin.

```text
coin_id
symbol
name
currency
price_now
price_24h_ago
price_7d_ago
price_30d_ago
return_24h_pct
return_7d_pct
return_30d_pct
volatility_7d
volatility_30d
last_updated_utc
```

#### gold_market_overview

Aggregated overview of the selected cryptocurrency universe.

```text
snapshot_date
currency
total_market_cap
total_volume_24h
average_return_24h_pct
best_performer_coin_id
best_performer_return_24h_pct
worst_performer_coin_id
worst_performer_return_24h_pct
coin_count
```

#### gold_crypto_rankings

Ranking metrics by coin.

```text
ranking_date
coin_id
symbol
name
currency
market_cap_rank
price_rank
volume_rank
return_24h_rank
return_7d_rank
return_30d_rank
```

## Pipeline Flow

The pipeline should execute the following steps:

```text
1. Load configuration
2. Validate configuration
3. Create a unique run_id
4. Extract cryptocurrency market data
5. Save the raw API response
6. Normalize raw data into the bronze layer
7. Clean and deduplicate data into the silver layer
8. Create gold analytical tables
9. Run data quality checks
10. Save execution logs
11. Refresh dashboard data
```

## Data Quality Checks

Required checks:

- `coin_id` must not be null.
- `symbol` must not be null.
- Prices must not be negative.
- Market capitalization must not be negative.
- Volume must not be negative.
- There must be no duplicate rows for the same coin and timestamp.
- `last_updated_utc` must not be in the future.
- Extraction row count must be greater than zero.
- All configured coins should be present or logged as missing.
- The latest prices table should contain one row per coin.

Critical failures should stop the pipeline. Non-critical issues should be logged as warnings.

## Error Handling

The pipeline should handle:

- API rate limits.
- Network errors.
- Server errors.
- Invalid responses.
- Empty responses.
- Timeout errors.
- Missing fields.

Retries with exponential backoff should be implemented for temporary errors.

Example behavior:

```text
Attempt 1 failed with HTTP 429.
Waiting before retry.
Attempt 2 failed with HTTP 429.
Waiting before retry.
Attempt 3 succeeded.
```

Errors should be logged clearly. The pipeline should not fail silently.

## Logging

Each pipeline run should log:

- `run_id`
- Start time
- End time
- Duration
- Selected coins
- Selected currency
- API endpoint used
- Number of raw records extracted
- Number of bronze records created
- Number of silver records created
- Number of gold records created
- Data quality results
- Final execution status

Example log entry:

```json
{
  "run_id": "20260706_103000_abc123",
  "step": "extract_market_data",
  "provider": "coingecko",
  "status": "success",
  "records_extracted": 10,
  "duration_seconds": 2.4
}
```

## Dashboard

Build a Streamlit dashboard that reads from the gold layer.

Required sections:

### Market Overview

Display:

- Total market cap.
- Total 24-hour volume.
- Average 24-hour return.
- Best performer.
- Worst performer.
- Number of tracked coins.

### Latest Prices

Display a table with:

- Coin
- Symbol
- Latest price
- Market cap
- 24-hour volume
- 24-hour change percentage
- Last updated timestamp

### Performance

Display price evolution by coin.

Suggested filters:

- Coin selector.
- Date range selector.
- Currency selector.

### Ranking

Show rankings by:

- Market cap.
- 24-hour volume.
- 24-hour return.
- 7-day return.
- 30-day return.

### Volatility

Show volatility metrics by coin.

Suggested visualizations:

- 7-day volatility bar chart.
- 30-day volatility bar chart.
- Return vs volatility scatter plot.

## Running the Project

### Create a virtual environment

```bash
python -m venv .venv
```

### Activate the environment

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### Install dependencies

```bash
pip install -e ".[dev]"
```

### Run tests

```bash
pytest
```

### Run the ETL pipeline

```bash
python -m crypto_etl.orchestration.run_pipeline
```

Optional example with parameters:

```bash
python -m crypto_etl.orchestration.run_pipeline --coins bitcoin,ethereum,solana --currency eur --historical-days 90
```

### Run the dashboard

```bash
streamlit run dashboard/app.py
```

## Acceptance Criteria

The project is complete when:

- The repository can be cloned and run locally.
- The setup instructions are clear.
- The pipeline extracts real cryptocurrency data.
- Raw responses are stored without modification.
- Bronze, silver, and gold layers are generated.
- Data quality checks are implemented.
- Logs are generated for each pipeline run.
- A Streamlit dashboard displays the gold metrics.
- The project can be demonstrated in a few minutes.
- Tests pass successfully.
- The code is modular and readable.

## First Milestone

Build the smallest working version first:

```text
CoinGecko API
  → Raw JSON
  → Silver market snapshot
  → Gold latest prices
  → Streamlit dashboard
```

After this works, extend the project with historical data, performance metrics, rankings, volatility, stronger orchestration, and deployment options.

## Future Improvements

Possible future enhancements:

- Add orchestration with Prefect, Airflow, or Dagster.
- Add dbt transformations.
- Add cloud storage.
- Add Snowflake, BigQuery, or Microsoft Fabric as an analytical backend.
- Add incremental loading.
- Add historical backfilling.
- Add real-time ingestion using WebSockets.
- Add alerts for large price movements.
- Add anomaly detection.
- Add portfolio simulation.
- Add volume anomaly detection.
- Add CI/CD with GitHub Actions.
- Add Docker Compose for local reproducibility.
- Add stronger data quality validation with Great Expectations or Soda.

## Development Principles

Prioritize:

- Clear folder structure.
- Config-driven execution.
- Separation between extraction, loading, transformation, and dashboard logic.
- Idempotent pipeline behavior.
- Raw data preservation.
- Clean logging.
- Basic test coverage.
- Reproducibility.
- Good documentation.
- Manual review and validation of any AI-assisted code or documentation.

Avoid:

- Hardcoded API parameters.
- Hardcoded coin lists inside functions.
- Transforming data before saving the raw response.
- Silent failures.
- Unclear filenames.
- One large script containing the full pipeline.
- Dashboard logic directly calling the API.