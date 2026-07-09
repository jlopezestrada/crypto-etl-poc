# crypto-etl-poc

A local-first cryptocurrency ETL proof of concept that extracts market data, preserves raw API responses, transforms them through bronze, silver, and gold analytical layers, and exposes the gold data through a Streamlit dashboard.

## What This Project Does

`crypto-etl-poc` demonstrates a small but complete data engineering workflow:

```text
CoinGecko API or sample fixture
  -> Raw JSON envelope
  -> Bronze normalized Parquet
  -> Silver cleaned Parquet
  -> Gold dashboard-ready Parquet
  -> Streamlit dashboard
```

The current MVP focuses on latest cryptocurrency market snapshots. Historical analytics, volatility, rankings, orchestration frameworks, Docker, DuckDB, dbt, and cloud storage are intentionally left for later phases.

## Current MVP Features

- Config-driven list of coins and currency.
- Live extraction from CoinGecko `/coins/markets`.
- Offline sample-data mode for demos and tests without network access.
- Raw response preservation before any transformation.
- Bronze, silver, and gold Parquet outputs.
- Data quality checks with critical failures and warning logs.
- Structured console logging with run IDs and durations.
- Streamlit dashboard that reads only gold Parquet files.
- Tests for config, extraction, raw loading, transformations, quality checks, pipeline smoke flow, and dashboard data validation.

## Quickstart

### 1. Create And Activate A Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### 2. Install Dependencies

```bash
pip install -e ".[dev]"
```

### 3. Run Tests

```bash
pytest
```

### 4. Run The Pipeline With Sample Data

Use this mode when offline or when you want a deterministic demo:

```bash
python -m crypto_etl.orchestration.run_pipeline --use-sample-data
```

### 5. Run The Pipeline Against CoinGecko

```bash
python -m crypto_etl.orchestration.run_pipeline
```

Optional overrides:

```bash
python -m crypto_etl.orchestration.run_pipeline --coins bitcoin,ethereum,solana --currency eur
```

### 6. Open The Dashboard

```bash
streamlit run dashboard/app.py
```

Run the pipeline before opening the dashboard so `data/gold/` contains the required Parquet files.

## Configuration

Runtime configuration lives in `config/config.yaml`:

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
api:
  base_url: https://api.coingecko.com/api/v3
  timeout_seconds: 10
  max_retries: 3
  retry_backoff_seconds: 1
paths:
  raw_dir: data/raw
  bronze_dir: data/bronze
  silver_dir: data/silver
  gold_dir: data/gold
```

The config loader validates that:

- `coins` is non-empty and contains no duplicates.
- `currency` is not blank.
- API retry and timeout values are positive.
- Output paths are represented as local paths.

## Project Structure

```text
crypto-etl-poc/
├── config/
│   └── config.yaml
├── dashboard/
│   ├── __init__.py
│   └── app.py
├── docs/
│   └── superpowers/
│       ├── plans/
│       └── specs/
├── src/
│   └── crypto_etl/
│       ├── clients/
│       │   └── coingecko_client.py
│       ├── load/
│       │   └── raw_loader.py
│       ├── orchestration/
│       │   └── run_pipeline.py
│       ├── quality/
│       │   └── checks.py
│       ├── transform/
│       │   ├── bronze_transform.py
│       │   ├── gold_transform.py
│       │   └── silver_transform.py
│       ├── utils/
│       │   └── time.py
│       ├── config.py
│       └── logging_config.py
├── tests/
│   ├── fixtures/
│   │   └── coingecko_market_sample.json
│   ├── test_bronze_transform.py
│   ├── test_coingecko_client.py
│   ├── test_config.py
│   ├── test_dashboard_data.py
│   ├── test_gold_transform.py
│   ├── test_pipeline_smoke.py
│   ├── test_quality_checks.py
│   ├── test_raw_loader.py
│   ├── test_silver_transform.py
│   └── test_time.py
├── pyproject.toml
└── README.md
```

Generated local outputs are written under `data/` and ignored by git.

## Pipeline Details

The pipeline entrypoint is `src/crypto_etl/orchestration/run_pipeline.py`.

It performs these steps:

1. Load and validate config.
2. Apply optional CLI overrides for coins and currency.
3. Create a unique `run_id`.
4. Extract current market data from CoinGecko, or load the committed sample fixture with `--use-sample-data`.
5. Build and write the raw response envelope.
6. Stop if the response is empty, after preserving the raw response.
7. Transform raw data into bronze Parquet.
8. Transform bronze into silver Parquet.
9. Run silver quality checks.
10. Build gold latest prices and market overview tables.
11. Run gold quality checks.
12. Log warnings, success/failure status, output paths, and duration.

## Data Layers

### Raw

The raw layer preserves the response before transformation.

Example path:

```text
data/raw/coingecko/market_data/year=YYYY/month=MM/day=DD/run_id=<run_id>/response.json
```

The raw envelope contains:

- `provider`
- `endpoint`
- `run_id`
- `requested_at_utc`
- `params`
- `response`

### Bronze

Bronze flattens each CoinGecko coin object into a row and keeps source-oriented field names.

Output:

```text
data/bronze/crypto_market_snapshot/data.parquet
```

Bronze includes metadata such as `run_id`, `provider`, `extracted_at_utc`, and `raw_file_path`.

### Silver

Silver cleans and standardizes bronze rows.

Output:

```text
data/silver/crypto_market_snapshot/data.parquet
```

Silver behavior:

- Converts numeric fields with `pandas.to_numeric`.
- Converts timestamps with UTC handling.
- Uppercases coin symbols.
- Adds normalized `currency`.
- Renames fields such as `current_price` to `price` and `total_volume` to `volume_24h`.
- Deduplicates rows by `coin_id` and `extracted_at_utc` while preserving source order.

### Gold

Gold creates dashboard-ready datasets.

Outputs:

```text
data/gold/gold_crypto_latest_prices.parquet
data/gold/gold_market_overview.parquet
```

`gold_crypto_latest_prices` contains the latest row per coin with:

- `coin_id`
- `symbol`
- `name`
- `currency`
- `latest_price`
- `market_cap`
- `market_cap_rank`
- `volume_24h`
- `price_change_pct_24h`
- `last_updated_utc`

`gold_market_overview` contains one aggregate row with:

- `snapshot_timestamp_utc`
- `currency`
- `total_market_cap`
- `total_volume_24h`
- `average_return_24h_pct`
- `best_performer_coin_id`
- `best_performer_return_24h_pct`
- `worst_performer_coin_id`
- `worst_performer_return_24h_pct`
- `coin_count`

## Data Quality Checks

Quality checks live in `src/crypto_etl/quality/checks.py`.

Critical issues stop the pipeline:

- Empty silver dataset.
- Null `coin_id`.
- Null `symbol`.
- Negative `price`.
- Negative `market_cap`.
- Negative `volume_24h`.
- Duplicate `coin_id` and `extracted_at_utc` rows.
- Future `last_updated_utc` values.
- Empty gold latest-prices dataset.
- Duplicate rows per coin in gold latest prices.

Warnings are logged but do not stop the pipeline:

- Configured coins missing from silver data.
- Configured coins missing from gold data.
- Gold overview coin count differing from latest-prices coin count.

## Dashboard

The dashboard lives in `dashboard/app.py` and is started with:

```bash
streamlit run dashboard/app.py
```

It reads only these gold files:

```text
data/gold/gold_crypto_latest_prices.parquet
data/gold/gold_market_overview.parquet
```

It does not call CoinGecko and does not run pipeline logic.

Dashboard sections:

- Market Overview: total market cap, 24h volume, average 24h return, best performer, worst performer, tracked coins.
- Latest Prices: table of dashboard-ready latest-price records.
- Top Movers: bar chart by 24h percentage change.
- Data Freshness: latest gold snapshot timestamp.

If gold data is missing, the dashboard shows a message explaining how to run the pipeline. If gold data is malformed, it shows a readable error instead of an uncaught stack trace.

## Error Handling And Retries

The CoinGecko client uses `httpx` and retries failed requests with exponential backoff:

```text
retry_backoff_seconds * (2 ** (attempt - 1))
```

The pipeline logs:

- Start event with `run_id`, coins, and currency.
- Raw, bronze, silver, and gold write events.
- Warning quality issues.
- Success event with duration.
- Failure event with duration and error.

## Development Commands

Run the full test suite:

```bash
pytest
```

Run linting:

```bash
ruff check .
```

Check formatting:

```bash
ruff format --check .
```

Run the offline smoke pipeline:

```bash
python -m crypto_etl.orchestration.run_pipeline --use-sample-data
```

## Current Limitations

- The MVP tracks latest market snapshots only.
- Historical price ingestion is not implemented yet.
- Performance metrics over 7d/30d are not implemented yet.
- Volatility analytics are not implemented yet.
- There is no Docker setup yet.
- There is no orchestration framework such as Prefect, Airflow, or Dagster yet.
- There is no DuckDB query layer yet.
- There is no CI/CD configuration yet.

## Future Improvements

Possible next steps:

- Add historical data ingestion.
- Add 7-day and 30-day returns.
- Add volatility metrics.
- Add rankings and richer dashboard charts.
- Add append-only partitioned writes.
- Add DuckDB as a local query layer over Parquet.
- Add Docker for reproducible local demos.
- Add GitHub Actions for tests and linting.
- Add orchestration with Prefect, Dagster, or Airflow.
- Add stronger data quality reporting.
- Add cloud storage or warehouse integrations.

## Design Documents

The approved design and implementation plan are stored under:

```text
docs/superpowers/specs/2026-07-06-layered-learning-mvp-design.md
docs/superpowers/plans/2026-07-06-layered-learning-mvp-implementation.md
```
