# Layered Learning MVP Design

Date: 2026-07-06

## Purpose

The MVP is a local-first learning product that demonstrates a complete cryptocurrency analytics ETL workflow without overengineering. It should be easy to run, easy to inspect, and structured like a professional data engineering project that can later evolve toward production quality.

The first product experience is:

1. Configure a small list of coins and a currency.
2. Run one pipeline command.
3. Extract live CoinGecko market data.
4. Preserve the original response in `data/raw` before any transformation.
5. Produce typed analytical datasets in `data/bronze`, `data/silver`, and `data/gold`.
6. Open a Streamlit dashboard that reads only from the gold layer.
7. Run tests that validate config loading, transformations, and data quality checks.
8. Demonstrate offline behavior through a committed sample fixture when live API access is unavailable.

## MVP Scope

The MVP includes:

- Live CoinGecko latest market extraction from `/coins/markets`.
- Config-driven coins, currency, output paths, and API settings.
- Raw JSON response preservation.
- Minimal bronze, silver, and gold layers written as Parquet.
- Streamlit dashboard backed only by gold Parquet datasets.
- Sample raw fixture for tests and an explicit offline demonstration mode.
- Basic retries, structured console logging, data quality checks, and test coverage.
- Python virtual environment setup using `pyproject.toml`.

The MVP excludes:

- Historical price ingestion and historical analytics.
- Docker and Docker Compose.
- Orchestration frameworks such as Prefect, Airflow, or Dagster.
- dbt, DuckDB as the primary storage layer, cloud storage, CI/CD, alerts, anomaly detection, and portfolio features.

## Architecture

The project will use small modules with clear responsibilities:

```text
config/config.yaml
  -> crypto_etl.config
  -> crypto_etl.clients.coingecko_client
  -> crypto_etl.load.raw_loader
  -> crypto_etl.transform.bronze_transform
  -> crypto_etl.transform.silver_transform
  -> crypto_etl.transform.gold_transform
  -> crypto_etl.quality.checks
  -> crypto_etl.orchestration.run_pipeline
  -> dashboard/app.py
```

Module responsibilities:

- `config.py` loads and validates runtime settings for coins, currency, API behavior, and output paths.
- `coingecko_client.py` talks to CoinGecko and returns response data plus request metadata.
- `raw_loader.py` writes the unmodified API response envelope to timestamped JSON.
- `bronze_transform.py` flattens raw CoinGecko records into normalized tabular rows with source metadata.
- `silver_transform.py` cleans types, standardizes symbols, deduplicates rows, and enforces basic validity.
- `gold_transform.py` creates dashboard-ready latest-price and market-overview datasets.
- `quality/checks.py` contains reusable validation checks used by the pipeline and tests.
- `run_pipeline.py` coordinates the pipeline steps and logs progress.
- `dashboard/app.py` reads gold Parquet files and never calls the API directly.

The implementation should prefer plain functions. Pydantic models are appropriate for configuration validation, but classes should not be introduced unless they clearly simplify the design.

## Data Flow

The MVP data flow is:

```text
CoinGecko /coins/markets
  -> raw JSON envelope
  -> bronze market snapshot parquet
  -> silver market snapshot parquet
  -> gold latest prices parquet
  -> gold market overview parquet
```

### Raw Layer

The raw layer stores the exact API response inside an envelope with metadata.

Path pattern:

```text
data/raw/coingecko/market_data/year=YYYY/month=MM/day=DD/run_id=<run_id>/response.json
```

Envelope fields:

- `provider`
- `endpoint`
- `run_id`
- `requested_at_utc`
- `params`
- `response`

No filtering, casting, renaming, or cleanup happens before the raw response is written.

### Bronze Layer

The bronze layer flattens each coin object from the raw response into one row. It keeps source-oriented names close to CoinGecko fields and adds source metadata.

Expected metadata fields:

- `run_id`
- `provider`
- `extracted_at_utc`
- `raw_file_path`

Output path:

```text
data/bronze/crypto_market_snapshot/
```

### Silver Layer

The silver layer converts bronze rows into clean analytical records.

Silver behavior:

- Convert timestamps into UTC-compatible timestamp values.
- Cast numeric fields into stable numeric types.
- Standardize symbols to uppercase.
- Rename source fields into analytical names such as `price`, `volume_24h`, and `price_change_pct_24h`.
- Remove duplicate rows for the same `coin_id` and `extracted_at_utc`.

Output path:

```text
data/silver/crypto_market_snapshot/
```

### Gold Layer

The gold layer contains dashboard-ready datasets.

`gold_crypto_latest_prices` contains the latest clean row per coin with:

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

`gold_market_overview` contains a one-row aggregate with:

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

Output path:

```text
data/gold/
```

For the MVP, dataset writes may overwrite the current Parquet files to keep reruns simple and inspectable. Incremental append behavior and partitioned idempotent writes are future production-path improvements.

## Pipeline Behavior

Primary command:

```bash
python -m crypto_etl.orchestration.run_pipeline
```

The MVP will support simple CLI overrides for coins and currency:

```bash
python -m crypto_etl.orchestration.run_pipeline --coins bitcoin,ethereum --currency eur
```

The MVP will also support an explicit sample-data mode for offline demonstrations and smoke tests:

```bash
python -m crypto_etl.orchestration.run_pipeline --use-sample-data
```

Sample-data mode reads the committed raw fixture instead of calling CoinGecko, then writes the same raw, bronze, silver, and gold outputs as a live run.

Pipeline steps:

1. Load and validate `config/config.yaml`.
2. Create a unique `run_id`.
3. Extract current market data from CoinGecko `/coins/markets`, or load the committed raw fixture in sample-data mode.
4. Save the raw response envelope before transformation.
5. Build bronze Parquet from the raw response.
6. Build silver Parquet from bronze.
7. Run quality checks against silver.
8. Build gold latest prices and market overview.
9. Run quality checks against gold.
10. Log step status, row counts, output paths, and total duration.

Failure behavior:

- Invalid config stops immediately.
- API, network, timeout, server, or rate-limit failures retry with exponential backoff.
- Empty API responses stop the pipeline.
- Critical data quality failures stop the pipeline.
- Missing configured coins are logged as warnings.
- Logs must clearly state whether the run succeeded or failed.

Logging for the MVP is structured console logging. File logs can be added later.

## Dashboard

The dashboard is a Streamlit product view over the gold layer only. It does not call CoinGecko and does not run transformations.

Initial sections:

- Market Overview: total market cap, total 24-hour volume, average 24-hour return, best performer, worst performer, and tracked coin count.
- Latest Prices: sortable table with coin name, symbol, latest price, market cap, rank, 24-hour volume, 24-hour percentage change, and last updated timestamp.
- Top Movers: best and worst 24-hour performers from the selected coin universe using a bar chart.
- Data Freshness: last update timestamp and clear status messaging.

Dashboard empty-state behavior:

- If gold data exists, render the dashboard.
- If gold data is missing, explain that the user should run `python -m crypto_etl.orchestration.run_pipeline` first, or `python -m crypto_etl.orchestration.run_pipeline --use-sample-data` when offline.
- If data is malformed, show a readable error instead of an uncaught stack trace.

Historical charts, date range selectors, volatility charts, and multi-currency switching are out of scope for the MVP because they depend on historical data.

## Testing And Quality

The test suite should prove the core data engineering behavior without trying to cover every future edge case.

Tests:

- `test_config.py`: loads valid config and rejects invalid coins, currency, or output paths.
- `test_bronze_transform.py`: converts a sample raw envelope into expected bronze rows.
- `test_silver_transform.py`: casts types, uppercases symbols, renames fields, and drops duplicates.
- `test_gold_transform.py`: builds latest prices and market overview correctly.
- `test_quality_checks.py`: detects null IDs, null symbols, negative metrics, duplicate coin/timestamp rows, empty datasets, and one-row-per-coin violations.
- `test_pipeline_smoke.py`: runs the pipeline path using `--use-sample-data` without hitting CoinGecko.

Critical quality checks:

- Silver dataset is not empty.
- `coin_id` is not null.
- `symbol` is not null.
- `price` is not negative.
- `market_cap` is not negative.
- `volume_24h` is not negative.
- There are no duplicate rows for the same `coin_id` and `extracted_at_utc`.
- Gold latest prices has one row per available coin.

Warning quality checks:

- Configured coins missing from extracted data.
- Gold overview coin count differs from configured coin count because a coin was missing upstream.

Tooling:

- `pytest` for tests.
- `ruff` for linting and formatting.
- `pyproject.toml` as the single dependency and tooling definition.

## Local Setup

The MVP targets Python virtual environment setup first.

Commands:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
python -m crypto_etl.orchestration.run_pipeline
streamlit run dashboard/app.py
```

Windows activation can be documented in the README, but the implementation plan should focus on the cross-platform Python package structure.

## Future Production Path

After the learning MVP works end to end, the project can evolve through these steps:

- Add historical data ingestion.
- Add performance, rankings, volatility, and historical dashboard charts.
- Add append-only and idempotent partitioned writes.
- Add DuckDB as a query layer over Parquet.
- Add Docker and Docker Compose.
- Add GitHub Actions for tests and linting.
- Add orchestration with Prefect, Dagster, or Airflow.
- Add richer data quality reporting.
- Add cloud/object storage and analytical warehouse options.

## Acceptance Criteria

The MVP is complete when:

- A new developer can clone, install, run tests, run the pipeline, and open the dashboard using documented commands.
- Live extraction retrieves current CoinGecko market data for configured coins.
- Raw responses are preserved before transformation.
- Bronze, silver, and gold datasets are inspectable on disk.
- The dashboard displays latest market overview, latest prices, top movers, and data freshness from gold data only.
- Tests pass without requiring live API access.
- Basic retries, logging, and data quality checks are present.
- The code is modular and readable enough to extend toward production without a major rewrite.
