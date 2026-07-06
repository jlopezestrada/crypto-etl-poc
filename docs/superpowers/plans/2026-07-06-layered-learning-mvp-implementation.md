# Layered Learning MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the local-first layered cryptocurrency ETL MVP from the approved design: CoinGecko or sample fixture input, raw JSON preservation, bronze/silver/gold Parquet outputs, quality checks, and a Streamlit dashboard.

**Architecture:** Use a small Python package under `src/crypto_etl` with plain functions and focused modules. The pipeline writes inspectable local files and the dashboard reads only gold Parquet outputs. Tests use a committed sample fixture and do not call CoinGecko.

**Tech Stack:** Python 3.11+, Pydantic v2, PyYAML, httpx, pandas, pyarrow, Streamlit, pytest, ruff.

## Global Constraints

- Python runtime: Python 3.11+.
- Setup path: Python virtual environment only for the MVP; no Docker files in this implementation.
- Source API: CoinGecko `/coins/markets` for live latest market snapshots.
- Storage: Parquet-first for bronze, silver, and gold datasets.
- Raw rule: write the raw response envelope before any transformation.
- Dashboard rule: `dashboard/app.py` reads gold Parquet data only and never calls CoinGecko.
- Offline rule: `--use-sample-data` must run the same pipeline path without network access.
- Test rule: `pytest` must pass without live API access.
- Scope rule: no historical ingestion, volatility, rankings, DuckDB, dbt, cloud storage, orchestration framework, CI, Docker, alerts, or portfolio features.
- Style rule: prefer plain functions; use Pydantic models for config validation.
- Logging rule: structured console logging is sufficient for the MVP.
- Git rule: each task ends with a conventional commit; stage only files touched by that task.

---

## File Structure

Create or modify these files during implementation:

- Modify: `.gitignore` to ignore generated local pipeline outputs under `data/raw/`, `data/bronze/`, `data/silver/`, and `data/gold/`.
- Create: `pyproject.toml` to define package metadata, runtime dependencies, dev dependencies, pytest config, and ruff config.
- Create: `config/config.yaml` as the default local runtime configuration.
- Create: `src/crypto_etl/__init__.py` for package metadata.
- Create: `src/crypto_etl/config.py` for Pydantic config models, YAML loading, and CLI override application.
- Create: `src/crypto_etl/logging_config.py` for structured console logging setup.
- Create: `src/crypto_etl/utils/__init__.py` and `src/crypto_etl/utils/time.py` for UTC timestamps and run IDs.
- Create: `src/crypto_etl/load/__init__.py` and `src/crypto_etl/load/raw_loader.py` for raw envelope construction, JSON writes, JSON reads, and sample fixture loading.
- Create: `src/crypto_etl/transform/__init__.py`, `src/crypto_etl/transform/bronze_transform.py`, `src/crypto_etl/transform/silver_transform.py`, and `src/crypto_etl/transform/gold_transform.py` for the layered transformations and Parquet writes.
- Create: `src/crypto_etl/quality/__init__.py` and `src/crypto_etl/quality/checks.py` for critical and warning data quality checks.
- Create: `src/crypto_etl/clients/__init__.py` and `src/crypto_etl/clients/coingecko_client.py` for live extraction with retries.
- Create: `src/crypto_etl/orchestration/__init__.py` and `src/crypto_etl/orchestration/run_pipeline.py` for CLI parsing and orchestration.
- Create: `dashboard/__init__.py` and `dashboard/app.py` for Streamlit dashboard rendering and testable gold-data loading.
- Create: `tests/fixtures/coingecko_market_sample.json` for sample offline input.
- Create: `tests/test_config.py`, `tests/test_time.py`, `tests/test_raw_loader.py`, `tests/test_bronze_transform.py`, `tests/test_silver_transform.py`, `tests/test_quality_checks.py`, `tests/test_gold_transform.py`, `tests/test_coingecko_client.py`, `tests/test_pipeline_smoke.py`, and `tests/test_dashboard_data.py`.
- Modify: `README.md` to document the implemented MVP quickstart commands.

---

### Task 1: Project Setup, Config, Logging, And Time Utilities

**Files:**
- Create: `pyproject.toml`
- Create: `config/config.yaml`
- Create: `src/crypto_etl/__init__.py`
- Create: `src/crypto_etl/config.py`
- Create: `src/crypto_etl/logging_config.py`
- Create: `src/crypto_etl/utils/__init__.py`
- Create: `src/crypto_etl/utils/time.py`
- Create: `tests/test_config.py`
- Create: `tests/test_time.py`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `ApiConfig`, `PathsConfig`, and `AppConfig` Pydantic models in `crypto_etl.config`.
- Produces: `load_config(path: Path = Path("config/config.yaml")) -> AppConfig`.
- Produces: `apply_overrides(config: AppConfig, coins: list[str] | None = None, currency: str | None = None) -> AppConfig`.
- Produces: `configure_logging(level: str = "INFO") -> None`.
- Produces: `utc_now() -> datetime` and `make_run_id(now: datetime | None = None) -> str`.
- Later tasks consume these interfaces without changing signatures.

- [ ] **Step 1: Write failing config and time tests**

Create `tests/test_config.py`:

```python
from pathlib import Path

import pytest
from pydantic import ValidationError

from crypto_etl.config import AppConfig, apply_overrides, load_config


def write_config(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_load_config_validates_and_normalizes_values(tmp_path: Path) -> None:
    config_path = write_config(
        tmp_path / "config.yaml",
        """
coins:
  - Bitcoin
  - ethereum
currency: EUR
api:
  base_url: https://api.coingecko.com/api/v3
  timeout_seconds: 5
  max_retries: 2
  retry_backoff_seconds: 0.1
paths:
  raw_dir: raw-output
  bronze_dir: bronze-output
  silver_dir: silver-output
  gold_dir: gold-output
""".strip(),
    )

    config = load_config(config_path)

    assert config.coins == ["bitcoin", "ethereum"]
    assert config.currency == "eur"
    assert config.api.base_url == "https://api.coingecko.com/api/v3"
    assert config.paths.raw_dir == Path("raw-output")


def test_load_config_rejects_empty_coins(tmp_path: Path) -> None:
    config_path = write_config(
        tmp_path / "config.yaml",
        """
coins: []
currency: eur
""".strip(),
    )

    with pytest.raises(ValidationError, match="coins"):
        load_config(config_path)


def test_load_config_rejects_blank_currency(tmp_path: Path) -> None:
    config_path = write_config(
        tmp_path / "config.yaml",
        """
coins:
  - bitcoin
currency: ""
""".strip(),
    )

    with pytest.raises(ValidationError, match="currency"):
        load_config(config_path)


def test_apply_overrides_returns_new_config() -> None:
    config = AppConfig(coins=["bitcoin"], currency="eur")

    updated = apply_overrides(config, coins=["solana", "ethereum"], currency="usd")

    assert updated.coins == ["solana", "ethereum"]
    assert updated.currency == "usd"
    assert config.coins == ["bitcoin"]
    assert config.currency == "eur"
```

Create `tests/test_time.py`:

```python
from datetime import UTC, datetime

from crypto_etl.utils.time import make_run_id, utc_now


def test_utc_now_returns_timezone_aware_utc_datetime() -> None:
    current = utc_now()

    assert current.tzinfo == UTC


def test_make_run_id_uses_timestamp_and_suffix() -> None:
    run_id = make_run_id(datetime(2026, 7, 6, 10, 30, 0, tzinfo=UTC))

    assert run_id.startswith("20260706_103000_")
    assert len(run_id) > len("20260706_103000_")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_config.py tests/test_time.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'crypto_etl'`.

- [ ] **Step 3: Add project setup and minimal implementation**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "crypto-etl-poc"
version = "0.1.0"
description = "Local-first cryptocurrency ETL proof of concept"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27",
    "pandas>=2.2",
    "pyarrow>=15",
    "pydantic>=2.7",
    "PyYAML>=6.0",
    "streamlit>=1.36",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2",
    "ruff>=0.5",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
```

Create `config/config.yaml`:

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

Create `src/crypto_etl/__init__.py`:

```python
"""Local-first cryptocurrency ETL proof of concept."""

__version__ = "0.1.0"
```

Create `src/crypto_etl/config.py`:

```python
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ApiConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    base_url: str = "https://api.coingecko.com/api/v3"
    timeout_seconds: float = Field(default=10.0, gt=0)
    max_retries: int = Field(default=3, ge=1)
    retry_backoff_seconds: float = Field(default=1.0, gt=0)


class PathsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw_dir: Path = Path("data/raw")
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    gold_dir: Path = Path("data/gold")


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    coins: list[str]
    currency: str
    api: ApiConfig = Field(default_factory=ApiConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)

    @field_validator("coins")
    @classmethod
    def validate_coins(cls, coins: list[str]) -> list[str]:
        normalized = [coin.strip().lower() for coin in coins if coin.strip()]
        if not normalized:
            raise ValueError("coins must contain at least one coin id")
        if len(set(normalized)) != len(normalized):
            raise ValueError("coins must not contain duplicates")
        return normalized

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, currency: str) -> str:
        normalized = currency.strip().lower()
        if not normalized:
            raise ValueError("currency must not be blank")
        return normalized


def load_config(path: Path = Path("config/config.yaml")) -> AppConfig:
    with path.open("r", encoding="utf-8") as config_file:
        data = yaml.safe_load(config_file) or {}
    return AppConfig.model_validate(data)


def apply_overrides(
    config: AppConfig,
    coins: list[str] | None = None,
    currency: str | None = None,
) -> AppConfig:
    updates: dict[str, object] = {}
    if coins is not None:
        updates["coins"] = coins
    if currency is not None:
        updates["currency"] = currency
    data = config.model_dump()
    data.update(updates)
    return AppConfig.model_validate(data)
```

Create `src/crypto_etl/logging_config.py`:

```python
import logging


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
```

Create `src/crypto_etl/utils/__init__.py`:

```python
"""Utility helpers for the crypto ETL package."""
```

Create `src/crypto_etl/utils/time.py`:

```python
from datetime import UTC, datetime
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


def make_run_id(now: datetime | None = None) -> str:
    timestamp = now or utc_now()
    return f"{timestamp:%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
```

Append these lines to `.gitignore`:

```gitignore

# Local pipeline outputs
data/raw/
data/bronze/
data/silver/
data/gold/
```

- [ ] **Step 4: Run tests and lint for this task**

Run:

```bash
python -m pip install -e ".[dev]"
pytest tests/test_config.py tests/test_time.py -v
ruff check pyproject.toml src tests
```

Expected: PASS for all commands.

- [ ] **Step 5: Commit task 1**

Run:

```bash
git add .gitignore pyproject.toml config/config.yaml src/crypto_etl/__init__.py src/crypto_etl/config.py src/crypto_etl/logging_config.py src/crypto_etl/utils tests/test_config.py tests/test_time.py
git commit -m "feat(config): add project setup and validated config"
```

---

### Task 2: Raw Fixture And Raw Loader

**Files:**
- Create: `src/crypto_etl/load/__init__.py`
- Create: `src/crypto_etl/load/raw_loader.py`
- Create: `tests/fixtures/coingecko_market_sample.json`
- Create: `tests/test_raw_loader.py`

**Interfaces:**
- Consumes: `utc_now()` from `crypto_etl.utils.time`.
- Produces: `build_raw_envelope(provider: str, endpoint: str, run_id: str, requested_at_utc: datetime, params: Mapping[str, Any], response: list[dict[str, Any]]) -> dict[str, Any]`.
- Produces: `write_raw_response(envelope: Mapping[str, Any], raw_dir: Path) -> Path`.
- Produces: `read_raw_response(path: Path) -> dict[str, Any]`.
- Produces: `load_sample_fixture(path: Path = Path("tests/fixtures/coingecko_market_sample.json")) -> dict[str, Any]`.
- Later bronze and pipeline tasks rely on the raw envelope keys: `provider`, `endpoint`, `run_id`, `requested_at_utc`, `params`, `response`.

- [ ] **Step 1: Add sample fixture and failing raw loader tests**

Create `tests/fixtures/coingecko_market_sample.json`:

```json
{
  "provider": "coingecko",
  "endpoint": "coins_markets",
  "run_id": "sample_20260706_103000",
  "requested_at_utc": "2026-07-06T10:30:00+00:00",
  "params": {
    "vs_currency": "eur",
    "ids": ["bitcoin", "ethereum", "solana"],
    "order": "market_cap_desc",
    "per_page": 250,
    "page": 1,
    "sparkline": false
  },
  "response": [
    {
      "id": "bitcoin",
      "symbol": "btc",
      "name": "Bitcoin",
      "current_price": 58000.12,
      "market_cap": 1140000000000,
      "market_cap_rank": 1,
      "fully_diluted_valuation": 1210000000000,
      "total_volume": 32000000000,
      "high_24h": 59000.0,
      "low_24h": 57000.0,
      "price_change_24h": 900.25,
      "price_change_percentage_24h": 1.58,
      "market_cap_change_24h": 18000000000,
      "market_cap_change_percentage_24h": 1.61,
      "circulating_supply": 19680000,
      "total_supply": 21000000,
      "max_supply": 21000000,
      "ath": 69000,
      "ath_change_percentage": -15.94,
      "ath_date": "2021-11-10T14:24:11.849Z",
      "atl": 67.81,
      "atl_change_percentage": 85435.2,
      "atl_date": "2013-07-06T00:00:00.000Z",
      "last_updated": "2026-07-06T10:28:00.000Z"
    },
    {
      "id": "ethereum",
      "symbol": "eth",
      "name": "Ethereum",
      "current_price": 3100.5,
      "market_cap": 372000000000,
      "market_cap_rank": 2,
      "fully_diluted_valuation": 372000000000,
      "total_volume": 18000000000,
      "high_24h": 3180.0,
      "low_24h": 3000.0,
      "price_change_24h": -25.5,
      "price_change_percentage_24h": -0.82,
      "market_cap_change_24h": -2900000000,
      "market_cap_change_percentage_24h": -0.77,
      "circulating_supply": 120100000,
      "total_supply": 120100000,
      "max_supply": null,
      "ath": 4878.26,
      "ath_change_percentage": -36.44,
      "ath_date": "2021-11-10T14:24:19.604Z",
      "atl": 0.432979,
      "atl_change_percentage": 715950.0,
      "atl_date": "2015-10-20T00:00:00.000Z",
      "last_updated": "2026-07-06T10:28:30.000Z"
    },
    {
      "id": "solana",
      "symbol": "sol",
      "name": "Solana",
      "current_price": 145.75,
      "market_cap": 68000000000,
      "market_cap_rank": 5,
      "fully_diluted_valuation": 85000000000,
      "total_volume": 4200000000,
      "high_24h": 150.0,
      "low_24h": 139.5,
      "price_change_24h": 4.8,
      "price_change_percentage_24h": 3.41,
      "market_cap_change_24h": 2200000000,
      "market_cap_change_percentage_24h": 3.34,
      "circulating_supply": 466000000,
      "total_supply": 582000000,
      "max_supply": null,
      "ath": 259.96,
      "ath_change_percentage": -43.93,
      "ath_date": "2021-11-06T21:54:35.825Z",
      "atl": 0.500801,
      "atl_change_percentage": 29010.0,
      "atl_date": "2020-05-11T19:35:23.449Z",
      "last_updated": "2026-07-06T10:29:00.000Z"
    }
  ]
}
```

Create `tests/test_raw_loader.py`:

```python
from datetime import UTC, datetime
from pathlib import Path

from crypto_etl.load.raw_loader import (
    build_raw_envelope,
    load_sample_fixture,
    read_raw_response,
    write_raw_response,
)


def test_build_raw_envelope_preserves_response() -> None:
    response = [{"id": "bitcoin", "current_price": 58000.12}]
    requested_at = datetime(2026, 7, 6, 10, 30, tzinfo=UTC)

    envelope = build_raw_envelope(
        provider="coingecko",
        endpoint="coins_markets",
        run_id="run-1",
        requested_at_utc=requested_at,
        params={"vs_currency": "eur", "ids": ["bitcoin"]},
        response=response,
    )

    assert envelope["provider"] == "coingecko"
    assert envelope["endpoint"] == "coins_markets"
    assert envelope["run_id"] == "run-1"
    assert envelope["requested_at_utc"] == "2026-07-06T10:30:00+00:00"
    assert envelope["params"] == {"vs_currency": "eur", "ids": ["bitcoin"]}
    assert envelope["response"] is response


def test_write_raw_response_partitions_by_requested_date(tmp_path: Path) -> None:
    envelope = build_raw_envelope(
        provider="coingecko",
        endpoint="coins_markets",
        run_id="run-1",
        requested_at_utc=datetime(2026, 7, 6, 10, 30, tzinfo=UTC),
        params={"vs_currency": "eur"},
        response=[{"id": "bitcoin"}],
    )

    output_path = write_raw_response(envelope, tmp_path)

    expected_path = (
        tmp_path
        / "coingecko"
        / "market_data"
        / "year=2026"
        / "month=07"
        / "day=06"
        / "run_id=run-1"
        / "response.json"
    )
    assert output_path == expected_path
    assert read_raw_response(output_path) == envelope


def test_load_sample_fixture_reads_committed_envelope() -> None:
    sample = load_sample_fixture()

    assert sample["provider"] == "coingecko"
    assert sample["endpoint"] == "coins_markets"
    assert [coin["id"] for coin in sample["response"]] == ["bitcoin", "ethereum", "solana"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_raw_loader.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'crypto_etl.load'`.

- [ ] **Step 3: Implement the raw loader**

Create `src/crypto_etl/load/__init__.py`:

```python
"""Loaders for persisted ETL data."""
```

Create `src/crypto_etl/load/raw_loader.py`:

```python
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


SAMPLE_FIXTURE_PATH = Path("tests/fixtures/coingecko_market_sample.json")


def build_raw_envelope(
    provider: str,
    endpoint: str,
    run_id: str,
    requested_at_utc: datetime,
    params: Mapping[str, Any],
    response: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "provider": provider,
        "endpoint": endpoint,
        "run_id": run_id,
        "requested_at_utc": requested_at_utc.isoformat(),
        "params": dict(params),
        "response": response,
    }


def write_raw_response(envelope: Mapping[str, Any], raw_dir: Path) -> Path:
    requested_at = datetime.fromisoformat(str(envelope["requested_at_utc"]))
    output_path = (
        raw_dir
        / str(envelope["provider"])
        / "market_data"
        / f"year={requested_at:%Y}"
        / f"month={requested_at:%m}"
        / f"day={requested_at:%d}"
        / f"run_id={envelope['run_id']}"
        / "response.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
    return output_path


def read_raw_response(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_sample_fixture(path: Path = SAMPLE_FIXTURE_PATH) -> dict[str, Any]:
    return read_raw_response(path)
```

- [ ] **Step 4: Run tests for this task**

Run:

```bash
pytest tests/test_raw_loader.py -v
ruff check src/crypto_etl/load tests/test_raw_loader.py
```

Expected: PASS for both commands.

- [ ] **Step 5: Commit task 2**

Run:

```bash
git add src/crypto_etl/load tests/fixtures/coingecko_market_sample.json tests/test_raw_loader.py
git commit -m "feat(raw): add raw fixture and loader"
```

---

### Task 3: Bronze Market Snapshot Transform

**Files:**
- Create: `src/crypto_etl/transform/__init__.py`
- Create: `src/crypto_etl/transform/bronze_transform.py`
- Create: `tests/test_bronze_transform.py`

**Interfaces:**
- Consumes: raw envelopes from `read_raw_response()` and `load_sample_fixture()`.
- Produces: `transform_raw_to_bronze(envelope: Mapping[str, Any], raw_file_path: Path) -> pd.DataFrame`.
- Produces: `write_bronze_snapshot(df: pd.DataFrame, bronze_dir: Path) -> Path`.
- Later silver task expects bronze columns to include `run_id`, `provider`, `extracted_at_utc`, `coin_id`, `symbol`, `name`, CoinGecko market fields, and `raw_file_path`.

- [ ] **Step 1: Write failing bronze transform tests**

Create `tests/test_bronze_transform.py`:

```python
from pathlib import Path

import pandas as pd

from crypto_etl.load.raw_loader import load_sample_fixture
from crypto_etl.transform.bronze_transform import transform_raw_to_bronze, write_bronze_snapshot


def test_transform_raw_to_bronze_flattens_market_records() -> None:
    envelope = load_sample_fixture()
    raw_file_path = Path("data/raw/sample/response.json")

    bronze = transform_raw_to_bronze(envelope, raw_file_path)

    assert list(bronze["coin_id"]) == ["bitcoin", "ethereum", "solana"]
    assert list(bronze["symbol"]) == ["btc", "eth", "sol"]
    assert bronze.loc[0, "run_id"] == "sample_20260706_103000"
    assert bronze.loc[0, "provider"] == "coingecko"
    assert bronze.loc[0, "extracted_at_utc"] == "2026-07-06T10:30:00+00:00"
    assert bronze.loc[0, "raw_file_path"] == str(raw_file_path)
    assert bronze.loc[0, "current_price"] == 58000.12


def test_write_bronze_snapshot_writes_parquet(tmp_path: Path) -> None:
    frame = pd.DataFrame([{"coin_id": "bitcoin", "current_price": 58000.12}])

    output_path = write_bronze_snapshot(frame, tmp_path)

    assert output_path == tmp_path / "crypto_market_snapshot" / "data.parquet"
    assert pd.read_parquet(output_path).to_dict("records") == frame.to_dict("records")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_bronze_transform.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'crypto_etl.transform'`.

- [ ] **Step 3: Implement bronze transform**

Create `src/crypto_etl/transform/__init__.py`:

```python
"""Layered data transformations for the crypto ETL pipeline."""
```

Create `src/crypto_etl/transform/bronze_transform.py`:

```python
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


BRONZE_FIELD_MAP = {
    "id": "coin_id",
    "symbol": "symbol",
    "name": "name",
    "current_price": "current_price",
    "market_cap": "market_cap",
    "market_cap_rank": "market_cap_rank",
    "fully_diluted_valuation": "fully_diluted_valuation",
    "total_volume": "total_volume",
    "high_24h": "high_24h",
    "low_24h": "low_24h",
    "price_change_24h": "price_change_24h",
    "price_change_percentage_24h": "price_change_percentage_24h",
    "market_cap_change_24h": "market_cap_change_24h",
    "market_cap_change_percentage_24h": "market_cap_change_percentage_24h",
    "circulating_supply": "circulating_supply",
    "total_supply": "total_supply",
    "max_supply": "max_supply",
    "ath": "ath",
    "ath_change_percentage": "ath_change_percentage",
    "ath_date": "ath_date",
    "atl": "atl",
    "atl_change_percentage": "atl_change_percentage",
    "atl_date": "atl_date",
    "last_updated": "last_updated",
}


def transform_raw_to_bronze(envelope: Mapping[str, Any], raw_file_path: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in envelope.get("response", []):
        row = {
            "run_id": envelope["run_id"],
            "provider": envelope["provider"],
            "extracted_at_utc": envelope["requested_at_utc"],
            "raw_file_path": str(raw_file_path),
        }
        row.update({target: record.get(source) for source, target in BRONZE_FIELD_MAP.items()})
        rows.append(row)
    return pd.DataFrame(rows)


def write_bronze_snapshot(df: pd.DataFrame, bronze_dir: Path) -> Path:
    output_path = bronze_dir / "crypto_market_snapshot" / "data.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return output_path
```

- [ ] **Step 4: Run tests for this task**

Run:

```bash
pytest tests/test_bronze_transform.py -v
ruff check src/crypto_etl/transform tests/test_bronze_transform.py
```

Expected: PASS for both commands.

- [ ] **Step 5: Commit task 3**

Run:

```bash
git add src/crypto_etl/transform tests/test_bronze_transform.py
git commit -m "feat(bronze): add market snapshot normalization"
```

---

### Task 4: Silver Market Snapshot Transform

**Files:**
- Create: `src/crypto_etl/transform/silver_transform.py`
- Create: `tests/test_silver_transform.py`

**Interfaces:**
- Consumes: bronze DataFrame from `transform_raw_to_bronze()`.
- Produces: `transform_bronze_to_silver(bronze_df: pd.DataFrame, currency: str) -> pd.DataFrame`.
- Produces: `write_silver_snapshot(df: pd.DataFrame, silver_dir: Path) -> Path`.
- Later quality and gold tasks expect silver columns: `coin_id`, `symbol`, `name`, `currency`, `price`, `market_cap`, `market_cap_rank`, `volume_24h`, `high_24h`, `low_24h`, `price_change_24h`, `price_change_pct_24h`, `market_cap_change_24h`, `market_cap_change_pct_24h`, `circulating_supply`, `total_supply`, `max_supply`, `ath`, `ath_date`, `atl`, `atl_date`, `last_updated_utc`, `extracted_at_utc`, `provider`.

- [ ] **Step 1: Write failing silver transform tests**

Create `tests/test_silver_transform.py`:

```python
from pathlib import Path

import pandas as pd

from crypto_etl.load.raw_loader import load_sample_fixture
from crypto_etl.transform.bronze_transform import transform_raw_to_bronze
from crypto_etl.transform.silver_transform import transform_bronze_to_silver, write_silver_snapshot


def test_transform_bronze_to_silver_cleans_types_and_names() -> None:
    bronze = transform_raw_to_bronze(load_sample_fixture(), Path("raw.json"))

    silver = transform_bronze_to_silver(bronze, currency="EUR")

    assert list(silver["coin_id"]) == ["bitcoin", "ethereum", "solana"]
    assert list(silver["symbol"]) == ["BTC", "ETH", "SOL"]
    assert set(silver["currency"]) == {"eur"}
    assert silver.loc[0, "price"] == 58000.12
    assert silver.loc[0, "volume_24h"] == 32000000000
    assert str(silver["last_updated_utc"].dtype).startswith("datetime64")
    assert str(silver["extracted_at_utc"].dtype).startswith("datetime64")


def test_transform_bronze_to_silver_drops_duplicate_coin_timestamp_rows() -> None:
    bronze = transform_raw_to_bronze(load_sample_fixture(), Path("raw.json"))
    duplicate = pd.concat([bronze, bronze.iloc[[0]]], ignore_index=True)

    silver = transform_bronze_to_silver(duplicate, currency="eur")

    assert len(silver) == 3
    assert silver["coin_id"].tolist() == ["bitcoin", "ethereum", "solana"]


def test_write_silver_snapshot_writes_parquet(tmp_path: Path) -> None:
    frame = pd.DataFrame([{"coin_id": "bitcoin", "price": 58000.12}])

    output_path = write_silver_snapshot(frame, tmp_path)

    assert output_path == tmp_path / "crypto_market_snapshot" / "data.parquet"
    assert pd.read_parquet(output_path).to_dict("records") == frame.to_dict("records")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_silver_transform.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'crypto_etl.transform.silver_transform'`.

- [ ] **Step 3: Implement silver transform**

Create `src/crypto_etl/transform/silver_transform.py`:

```python
from pathlib import Path

import pandas as pd


SILVER_COLUMNS = [
    "coin_id",
    "symbol",
    "name",
    "currency",
    "price",
    "market_cap",
    "market_cap_rank",
    "volume_24h",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_pct_24h",
    "market_cap_change_24h",
    "market_cap_change_pct_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "ath_date",
    "atl",
    "atl_date",
    "last_updated_utc",
    "extracted_at_utc",
    "provider",
]

RENAME_MAP = {
    "current_price": "price",
    "total_volume": "volume_24h",
    "price_change_percentage_24h": "price_change_pct_24h",
    "market_cap_change_percentage_24h": "market_cap_change_pct_24h",
    "last_updated": "last_updated_utc",
}

NUMERIC_COLUMNS = [
    "price",
    "market_cap",
    "market_cap_rank",
    "volume_24h",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_pct_24h",
    "market_cap_change_24h",
    "market_cap_change_pct_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "atl",
]


def transform_bronze_to_silver(bronze_df: pd.DataFrame, currency: str) -> pd.DataFrame:
    silver = bronze_df.rename(columns=RENAME_MAP).copy()
    silver["currency"] = currency.strip().lower()
    silver["symbol"] = silver["symbol"].astype("string").str.upper()

    for column in NUMERIC_COLUMNS:
        if column in silver.columns:
            silver[column] = pd.to_numeric(silver[column], errors="coerce")

    for column in ["last_updated_utc", "extracted_at_utc", "ath_date", "atl_date"]:
        if column in silver.columns:
            silver[column] = pd.to_datetime(silver[column], utc=True, errors="coerce")

    silver = silver.drop_duplicates(subset=["coin_id", "extracted_at_utc"], keep="last")
    return silver.reindex(columns=SILVER_COLUMNS)


def write_silver_snapshot(df: pd.DataFrame, silver_dir: Path) -> Path:
    output_path = silver_dir / "crypto_market_snapshot" / "data.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return output_path
```

- [ ] **Step 4: Run tests for this task**

Run:

```bash
pytest tests/test_silver_transform.py -v
ruff check src/crypto_etl/transform/silver_transform.py tests/test_silver_transform.py
```

Expected: PASS for both commands.

- [ ] **Step 5: Commit task 4**

Run:

```bash
git add src/crypto_etl/transform/silver_transform.py tests/test_silver_transform.py
git commit -m "feat(silver): add clean market snapshot transform"
```

---

### Task 5: Data Quality Checks

**Files:**
- Create: `src/crypto_etl/quality/__init__.py`
- Create: `src/crypto_etl/quality/checks.py`
- Create: `tests/test_quality_checks.py`

**Interfaces:**
- Consumes: silver DataFrame from `transform_bronze_to_silver()` and gold DataFrames from Task 6.
- Produces: `QualityIssue` dataclass with fields `severity: str`, `check_name: str`, and `message: str`.
- Produces: `QualityCheckError` exception.
- Produces: `run_silver_quality_checks(df: pd.DataFrame, configured_coins: Sequence[str]) -> list[QualityIssue]`.
- Produces: `run_gold_quality_checks(latest_df: pd.DataFrame, overview_df: pd.DataFrame, configured_coins: Sequence[str]) -> list[QualityIssue]`.
- Produces: `raise_for_critical_issues(issues: Sequence[QualityIssue]) -> None`.

- [ ] **Step 1: Write failing quality check tests**

Create `tests/test_quality_checks.py`:

```python
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from crypto_etl.quality.checks import (
    QualityCheckError,
    raise_for_critical_issues,
    run_gold_quality_checks,
    run_silver_quality_checks,
)


def valid_silver_frame() -> pd.DataFrame:
    extracted_at = pd.Timestamp(datetime(2026, 7, 6, 10, 30, tzinfo=UTC))
    return pd.DataFrame(
        [
            {
                "coin_id": "bitcoin",
                "symbol": "BTC",
                "price": 58000.12,
                "market_cap": 1140000000000,
                "volume_24h": 32000000000,
                "extracted_at_utc": extracted_at,
                "last_updated_utc": extracted_at,
            },
            {
                "coin_id": "ethereum",
                "symbol": "ETH",
                "price": 3100.5,
                "market_cap": 372000000000,
                "volume_24h": 18000000000,
                "extracted_at_utc": extracted_at,
                "last_updated_utc": extracted_at,
            },
        ]
    )


def test_silver_quality_checks_return_no_issues_for_valid_data() -> None:
    issues = run_silver_quality_checks(valid_silver_frame(), ["bitcoin", "ethereum"])

    assert issues == []


def test_silver_quality_checks_detect_critical_failures() -> None:
    frame = valid_silver_frame()
    frame.loc[0, "coin_id"] = None
    frame.loc[1, "price"] = -1

    issues = run_silver_quality_checks(frame, ["bitcoin", "ethereum"])

    assert {issue.check_name for issue in issues if issue.severity == "critical"} == {
        "coin_id_not_null",
        "price_non_negative",
    }


def test_silver_quality_checks_detect_duplicates_and_future_timestamps() -> None:
    frame = pd.concat([valid_silver_frame(), valid_silver_frame().iloc[[0]]], ignore_index=True)
    frame.loc[0, "last_updated_utc"] = pd.Timestamp(datetime.now(UTC) + timedelta(days=1))

    issues = run_silver_quality_checks(frame, ["bitcoin", "ethereum"])

    assert {issue.check_name for issue in issues if issue.severity == "critical"} == {
        "duplicate_coin_extraction_timestamp",
        "last_updated_not_in_future",
    }


def test_silver_quality_checks_warn_for_missing_configured_coin() -> None:
    issues = run_silver_quality_checks(valid_silver_frame(), ["bitcoin", "ethereum", "solana"])

    assert [(issue.severity, issue.check_name) for issue in issues] == [
        ("warning", "configured_coins_present")
    ]


def test_gold_quality_checks_latest_prices_one_row_per_coin() -> None:
    latest = pd.DataFrame(
        [
            {"coin_id": "bitcoin", "latest_price": 58000.12},
            {"coin_id": "bitcoin", "latest_price": 58000.12},
        ]
    )
    overview = pd.DataFrame([{"coin_count": 1}])

    issues = run_gold_quality_checks(latest, overview, ["bitcoin"])

    assert [(issue.severity, issue.check_name) for issue in issues] == [
        ("critical", "latest_prices_one_row_per_coin")
    ]


def test_raise_for_critical_issues_raises_for_critical_issue() -> None:
    issues = run_silver_quality_checks(pd.DataFrame(), ["bitcoin"])

    with pytest.raises(QualityCheckError, match="silver_not_empty"):
        raise_for_critical_issues(issues)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_quality_checks.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'crypto_etl.quality'`.

- [ ] **Step 3: Implement quality checks**

Create `src/crypto_etl/quality/__init__.py`:

```python
"""Data quality checks for ETL outputs."""
```

Create `src/crypto_etl/quality/checks.py`:

```python
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Sequence

import pandas as pd

Severity = Literal["critical", "warning"]


@dataclass(frozen=True)
class QualityIssue:
    severity: Severity
    check_name: str
    message: str


class QualityCheckError(RuntimeError):
    pass


def _issue(severity: Severity, check_name: str, message: str) -> QualityIssue:
    return QualityIssue(severity=severity, check_name=check_name, message=message)


def run_silver_quality_checks(
    df: pd.DataFrame,
    configured_coins: Sequence[str],
) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    if df.empty:
        return [_issue("critical", "silver_not_empty", "silver dataset must not be empty")]

    if df["coin_id"].isna().any():
        issues.append(_issue("critical", "coin_id_not_null", "coin_id must not be null"))
    if df["symbol"].isna().any():
        issues.append(_issue("critical", "symbol_not_null", "symbol must not be null"))

    for column, check_name in [
        ("price", "price_non_negative"),
        ("market_cap", "market_cap_non_negative"),
        ("volume_24h", "volume_24h_non_negative"),
    ]:
        if (df[column] < 0).any():
            issues.append(_issue("critical", check_name, f"{column} must not be negative"))

    if df.duplicated(subset=["coin_id", "extracted_at_utc"]).any():
        issues.append(
            _issue(
                "critical",
                "duplicate_coin_extraction_timestamp",
                "silver must not contain duplicate coin_id and extracted_at_utc rows",
            )
        )

    current_time = pd.Timestamp(datetime.now(UTC))
    if (pd.to_datetime(df["last_updated_utc"], utc=True) > current_time).any():
        issues.append(
            _issue(
                "critical",
                "last_updated_not_in_future",
                "last_updated_utc must not be in the future",
            )
        )

    missing = sorted(set(configured_coins) - set(df["coin_id"].dropna()))
    if missing:
        issues.append(
            _issue(
                "warning",
                "configured_coins_present",
                f"configured coins missing from data: {', '.join(missing)}",
            )
        )

    return issues


def run_gold_quality_checks(
    latest_df: pd.DataFrame,
    overview_df: pd.DataFrame,
    configured_coins: Sequence[str],
) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    if latest_df.empty:
        issues.append(
            _issue("critical", "latest_prices_not_empty", "latest prices must not be empty")
        )
        return issues

    if latest_df["coin_id"].duplicated().any():
        issues.append(
            _issue(
                "critical",
                "latest_prices_one_row_per_coin",
                "latest prices must contain one row per coin",
            )
        )

    if not overview_df.empty:
        available_count = len(set(latest_df["coin_id"].dropna()))
        overview_count = int(overview_df.iloc[0]["coin_count"])
        if overview_count != available_count:
            issues.append(
                _issue(
                    "warning",
                    "overview_coin_count_matches_latest",
                    "overview coin_count differs from latest prices coin count",
                )
            )

    missing = sorted(set(configured_coins) - set(latest_df["coin_id"].dropna()))
    if missing:
        issues.append(
            _issue(
                "warning",
                "gold_configured_coins_present",
                f"configured coins missing from gold data: {', '.join(missing)}",
            )
        )

    return issues


def raise_for_critical_issues(issues: Sequence[QualityIssue]) -> None:
    critical = [issue for issue in issues if issue.severity == "critical"]
    if critical:
        details = "; ".join(f"{issue.check_name}: {issue.message}" for issue in critical)
        raise QualityCheckError(details)
```

- [ ] **Step 4: Run tests for this task**

Run:

```bash
pytest tests/test_quality_checks.py -v
ruff check src/crypto_etl/quality tests/test_quality_checks.py
```

Expected: PASS for both commands.

- [ ] **Step 5: Commit task 5**

Run:

```bash
git add src/crypto_etl/quality tests/test_quality_checks.py
git commit -m "feat(quality): add market data quality checks"
```

---

### Task 6: Gold Latest Prices And Market Overview

**Files:**
- Create: `src/crypto_etl/transform/gold_transform.py`
- Create: `tests/test_gold_transform.py`

**Interfaces:**
- Consumes: silver DataFrame from `transform_bronze_to_silver()`.
- Produces: `build_latest_prices(silver_df: pd.DataFrame) -> pd.DataFrame`.
- Produces: `build_market_overview(latest_df: pd.DataFrame) -> pd.DataFrame`.
- Produces: `write_gold_dataset(df: pd.DataFrame, gold_dir: Path, dataset_name: str) -> Path`.
- Pipeline and dashboard expect gold files at `data/gold/gold_crypto_latest_prices.parquet` and `data/gold/gold_market_overview.parquet`.

- [ ] **Step 1: Write failing gold transform tests**

Create `tests/test_gold_transform.py`:

```python
from pathlib import Path

import pandas as pd

from crypto_etl.load.raw_loader import load_sample_fixture
from crypto_etl.transform.bronze_transform import transform_raw_to_bronze
from crypto_etl.transform.gold_transform import (
    build_latest_prices,
    build_market_overview,
    write_gold_dataset,
)
from crypto_etl.transform.silver_transform import transform_bronze_to_silver


def sample_silver() -> pd.DataFrame:
    bronze = transform_raw_to_bronze(load_sample_fixture(), Path("raw.json"))
    return transform_bronze_to_silver(bronze, currency="eur")


def test_build_latest_prices_selects_latest_row_per_coin() -> None:
    silver = sample_silver()
    older = silver.copy()
    older["extracted_at_utc"] = older["extracted_at_utc"] - pd.Timedelta(hours=1)
    older["price"] = older["price"] - 100

    latest = build_latest_prices(pd.concat([older, silver], ignore_index=True))

    assert list(latest["coin_id"]) == ["bitcoin", "ethereum", "solana"]
    assert list(latest["latest_price"]) == [58000.12, 3100.5, 145.75]
    assert "volume_24h" in latest.columns


def test_build_market_overview_aggregates_latest_prices() -> None:
    latest = build_latest_prices(sample_silver())

    overview = build_market_overview(latest)

    assert len(overview) == 1
    row = overview.iloc[0]
    assert row["currency"] == "eur"
    assert row["coin_count"] == 3
    assert row["total_market_cap"] == 1580000000000
    assert row["total_volume_24h"] == 54200000000
    assert row["best_performer_coin_id"] == "solana"
    assert row["worst_performer_coin_id"] == "ethereum"


def test_write_gold_dataset_writes_named_parquet(tmp_path: Path) -> None:
    frame = pd.DataFrame([{"coin_id": "bitcoin", "latest_price": 58000.12}])

    output_path = write_gold_dataset(frame, tmp_path, "gold_crypto_latest_prices")

    assert output_path == tmp_path / "gold_crypto_latest_prices.parquet"
    assert pd.read_parquet(output_path).to_dict("records") == frame.to_dict("records")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_gold_transform.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'crypto_etl.transform.gold_transform'`.

- [ ] **Step 3: Implement gold transforms**

Create `src/crypto_etl/transform/gold_transform.py`:

```python
from pathlib import Path

import pandas as pd


LATEST_PRICE_COLUMNS = [
    "coin_id",
    "symbol",
    "name",
    "currency",
    "latest_price",
    "market_cap",
    "market_cap_rank",
    "volume_24h",
    "price_change_pct_24h",
    "last_updated_utc",
]


def build_latest_prices(silver_df: pd.DataFrame) -> pd.DataFrame:
    sorted_df = silver_df.sort_values(["coin_id", "extracted_at_utc"])
    latest = sorted_df.drop_duplicates(subset=["coin_id"], keep="last").copy()
    latest = latest.rename(columns={"price": "latest_price"})
    return (
        latest.reindex(columns=LATEST_PRICE_COLUMNS)
        .sort_values("market_cap_rank")
        .reset_index(drop=True)
    )


def build_market_overview(latest_df: pd.DataFrame) -> pd.DataFrame:
    best = latest_df.sort_values("price_change_pct_24h", ascending=False).iloc[0]
    worst = latest_df.sort_values("price_change_pct_24h", ascending=True).iloc[0]
    snapshot_timestamp = pd.to_datetime(latest_df["last_updated_utc"], utc=True).max()
    return pd.DataFrame(
        [
            {
                "snapshot_timestamp_utc": snapshot_timestamp,
                "currency": latest_df["currency"].iloc[0],
                "total_market_cap": latest_df["market_cap"].sum(),
                "total_volume_24h": latest_df["volume_24h"].sum(),
                "average_return_24h_pct": latest_df["price_change_pct_24h"].mean(),
                "best_performer_coin_id": best["coin_id"],
                "best_performer_return_24h_pct": best["price_change_pct_24h"],
                "worst_performer_coin_id": worst["coin_id"],
                "worst_performer_return_24h_pct": worst["price_change_pct_24h"],
                "coin_count": len(latest_df),
            }
        ]
    )


def write_gold_dataset(df: pd.DataFrame, gold_dir: Path, dataset_name: str) -> Path:
    output_path = gold_dir / f"{dataset_name}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return output_path
```

- [ ] **Step 4: Run tests for this task**

Run:

```bash
pytest tests/test_gold_transform.py -v
ruff check src/crypto_etl/transform/gold_transform.py tests/test_gold_transform.py
```

Expected: PASS for both commands.

- [ ] **Step 5: Commit task 6**

Run:

```bash
git add src/crypto_etl/transform/gold_transform.py tests/test_gold_transform.py
git commit -m "feat(gold): add latest prices and overview transforms"
```

---

### Task 7: CoinGecko Client And Pipeline Orchestration

**Files:**
- Create: `src/crypto_etl/clients/__init__.py`
- Create: `src/crypto_etl/clients/coingecko_client.py`
- Create: `src/crypto_etl/orchestration/__init__.py`
- Create: `src/crypto_etl/orchestration/run_pipeline.py`
- Create: `tests/test_coingecko_client.py`
- Create: `tests/test_pipeline_smoke.py`

**Interfaces:**
- Consumes: `AppConfig`, `load_config()`, `apply_overrides()`, `build_raw_envelope()`, raw writers, transforms, and quality checks.
- Produces: `fetch_coin_markets(coins: Sequence[str], currency: str, api_config: ApiConfig, transport: httpx.BaseTransport | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]`.
- Produces: `PipelineResult` dataclass with `run_id: str`, `raw_path: Path`, `bronze_path: Path`, `silver_path: Path`, `latest_prices_path: Path`, `market_overview_path: Path`, and `quality_issues: list[QualityIssue]`.
- Produces: `run_pipeline(config_path: Path = Path("config/config.yaml"), coins_override: list[str] | None = None, currency_override: str | None = None, use_sample_data: bool = False, sample_path: Path = Path("tests/fixtures/coingecko_market_sample.json")) -> PipelineResult`.
- Produces: CLI entrypoint through `main(argv: Sequence[str] | None = None) -> int`.

- [ ] **Step 1: Write failing client and pipeline smoke tests**

Create `tests/test_coingecko_client.py`:

```python
import httpx

from crypto_etl.clients.coingecko_client import fetch_coin_markets
from crypto_etl.config import ApiConfig


def test_fetch_coin_markets_builds_expected_request() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[{"id": "bitcoin"}])

    response, params = fetch_coin_markets(
        coins=["bitcoin", "ethereum"],
        currency="eur",
        api_config=ApiConfig(timeout_seconds=1, max_retries=1, retry_backoff_seconds=0.01),
        transport=httpx.MockTransport(handler),
    )

    assert response == [{"id": "bitcoin"}]
    assert params["ids"] == ["bitcoin", "ethereum"]
    assert requests[0].url.path == "/api/v3/coins/markets"
    assert requests[0].url.params["vs_currency"] == "eur"
    assert requests[0].url.params["ids"] == "bitcoin,ethereum"


def test_fetch_coin_markets_retries_temporary_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, json={"error": "rate limited"})
        return httpx.Response(200, json=[{"id": "bitcoin"}])

    response, _ = fetch_coin_markets(
        coins=["bitcoin"],
        currency="eur",
        api_config=ApiConfig(timeout_seconds=1, max_retries=2, retry_backoff_seconds=0.01),
        transport=httpx.MockTransport(handler),
    )

    assert response == [{"id": "bitcoin"}]
    assert calls == 2
```

Create `tests/test_pipeline_smoke.py`:

```python
from pathlib import Path

import pandas as pd

from crypto_etl.orchestration.run_pipeline import run_pipeline


def write_smoke_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
coins:
  - bitcoin
  - ethereum
  - solana
currency: eur
api:
  base_url: https://api.coingecko.com/api/v3
  timeout_seconds: 1
  max_retries: 1
  retry_backoff_seconds: 0.01
paths:
  raw_dir: {tmp_path / "raw"}
  bronze_dir: {tmp_path / "bronze"}
  silver_dir: {tmp_path / "silver"}
  gold_dir: {tmp_path / "gold"}
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_run_pipeline_with_sample_data_writes_all_layers(tmp_path: Path) -> None:
    result = run_pipeline(config_path=write_smoke_config(tmp_path), use_sample_data=True)

    assert result.raw_path.exists()
    assert result.bronze_path.exists()
    assert result.silver_path.exists()
    assert result.latest_prices_path.exists()
    assert result.market_overview_path.exists()
    assert result.quality_issues == []

    latest = pd.read_parquet(result.latest_prices_path)
    overview = pd.read_parquet(result.market_overview_path)
    assert latest["coin_id"].tolist() == ["bitcoin", "ethereum", "solana"]
    assert overview.iloc[0]["coin_count"] == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_coingecko_client.py tests/test_pipeline_smoke.py -v
```

Expected: FAIL during import with `ModuleNotFoundError` for `crypto_etl.clients` or `crypto_etl.orchestration`.

- [ ] **Step 3: Implement CoinGecko client**

Create `src/crypto_etl/clients/__init__.py`:

```python
"""API clients for cryptocurrency data providers."""
```

Create `src/crypto_etl/clients/coingecko_client.py`:

```python
import logging
import time
from typing import Any, Sequence

import httpx

from crypto_etl.config import ApiConfig

LOGGER = logging.getLogger(__name__)


def fetch_coin_markets(
    coins: Sequence[str],
    currency: str,
    api_config: ApiConfig,
    transport: httpx.BaseTransport | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: dict[str, Any] = {
        "vs_currency": currency,
        "ids": list(coins),
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": False,
    }
    request_params = {**params, "ids": ",".join(coins)}

    last_error: Exception | None = None
    with httpx.Client(
        base_url=api_config.base_url,
        timeout=api_config.timeout_seconds,
        transport=transport,
    ) as client:
        for attempt in range(1, api_config.max_retries + 1):
            try:
                response = client.get("coins/markets", params=request_params)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("CoinGecko /coins/markets response must be a list")
                return payload, params
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                LOGGER.warning("coingecko_request_failed attempt=%s error=%s", attempt, exc)
                if attempt == api_config.max_retries:
                    break
                time.sleep(api_config.retry_backoff_seconds * attempt)

    raise RuntimeError("CoinGecko market data request failed") from last_error
```

- [ ] **Step 4: Implement pipeline orchestration**

Create `src/crypto_etl/orchestration/__init__.py`:

```python
"""Pipeline orchestration entrypoints."""
```

Create `src/crypto_etl/orchestration/run_pipeline.py`:

```python
import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from crypto_etl.clients.coingecko_client import fetch_coin_markets
from crypto_etl.config import apply_overrides, load_config
from crypto_etl.load.raw_loader import build_raw_envelope, load_sample_fixture, write_raw_response
from crypto_etl.logging_config import configure_logging
from crypto_etl.quality.checks import (
    QualityIssue,
    raise_for_critical_issues,
    run_gold_quality_checks,
    run_silver_quality_checks,
)
from crypto_etl.transform.bronze_transform import transform_raw_to_bronze, write_bronze_snapshot
from crypto_etl.transform.gold_transform import (
    build_latest_prices,
    build_market_overview,
    write_gold_dataset,
)
from crypto_etl.transform.silver_transform import transform_bronze_to_silver, write_silver_snapshot
from crypto_etl.utils.time import make_run_id, utc_now

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    raw_path: Path
    bronze_path: Path
    silver_path: Path
    latest_prices_path: Path
    market_overview_path: Path
    quality_issues: list[QualityIssue]


def run_pipeline(
    config_path: Path = Path("config/config.yaml"),
    coins_override: list[str] | None = None,
    currency_override: str | None = None,
    use_sample_data: bool = False,
    sample_path: Path = Path("tests/fixtures/coingecko_market_sample.json"),
) -> PipelineResult:
    config = apply_overrides(load_config(config_path), coins_override, currency_override)
    requested_at = utc_now()
    run_id = make_run_id(requested_at)
    LOGGER.info("pipeline_started run_id=%s coins=%s currency=%s", run_id, config.coins, config.currency)

    if use_sample_data:
        sample = load_sample_fixture(sample_path)
        response = sample["response"]
        params = sample["params"]
        params["ids"] = config.coins
        params["vs_currency"] = config.currency
    else:
        response, params = fetch_coin_markets(config.coins, config.currency, config.api)

    if not response:
        raise RuntimeError("extraction returned zero records")

    envelope = build_raw_envelope(
        provider="coingecko",
        endpoint="coins_markets",
        run_id=run_id,
        requested_at_utc=requested_at,
        params=params,
        response=response,
    )
    raw_path = write_raw_response(envelope, config.paths.raw_dir)
    LOGGER.info("raw_written path=%s records=%s", raw_path, len(response))

    bronze = transform_raw_to_bronze(envelope, raw_path)
    bronze_path = write_bronze_snapshot(bronze, config.paths.bronze_dir)
    LOGGER.info("bronze_written path=%s records=%s", bronze_path, len(bronze))

    silver = transform_bronze_to_silver(bronze, config.currency)
    silver_path = write_silver_snapshot(silver, config.paths.silver_dir)
    silver_issues = run_silver_quality_checks(silver, config.coins)
    raise_for_critical_issues(silver_issues)
    LOGGER.info(
        "silver_written path=%s records=%s issues=%s",
        silver_path,
        len(silver),
        len(silver_issues),
    )

    latest = build_latest_prices(silver)
    overview = build_market_overview(latest)
    latest_path = write_gold_dataset(latest, config.paths.gold_dir, "gold_crypto_latest_prices")
    overview_path = write_gold_dataset(overview, config.paths.gold_dir, "gold_market_overview")
    gold_issues = run_gold_quality_checks(latest, overview, config.coins)
    raise_for_critical_issues(gold_issues)
    LOGGER.info(
        "gold_written latest=%s overview=%s issues=%s",
        latest_path,
        overview_path,
        len(gold_issues),
    )
    LOGGER.info("pipeline_succeeded run_id=%s", run_id)

    return PipelineResult(
        run_id=run_id,
        raw_path=raw_path,
        bronze_path=bronze_path,
        silver_path=silver_path,
        latest_prices_path=latest_path,
        market_overview_path=overview_path,
        quality_issues=silver_issues + gold_issues,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the crypto ETL pipeline")
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    parser.add_argument("--coins", type=str, default=None)
    parser.add_argument("--currency", type=str, default=None)
    parser.add_argument("--use-sample-data", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    coins_override = args.coins.split(",") if args.coins else None
    run_pipeline(
        config_path=args.config,
        coins_override=coins_override,
        currency_override=args.currency,
        use_sample_data=args.use_sample_data,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run tests for this task**

Run:

```bash
pytest tests/test_coingecko_client.py tests/test_pipeline_smoke.py -v
ruff check src/crypto_etl/clients src/crypto_etl/orchestration tests/test_coingecko_client.py tests/test_pipeline_smoke.py
```

Expected: PASS for both commands.

- [ ] **Step 6: Run sample pipeline manually**

Run:

```bash
python -m crypto_etl.orchestration.run_pipeline --use-sample-data
```

Expected: exit code 0, console logs include `pipeline_succeeded`, and local files exist under `data/raw/`, `data/bronze/`, `data/silver/`, and `data/gold/`.

- [ ] **Step 7: Commit task 7**

Run:

```bash
git add src/crypto_etl/clients src/crypto_etl/orchestration tests/test_coingecko_client.py tests/test_pipeline_smoke.py
git commit -m "feat(pipeline): add coingecko extraction and orchestration"
```

---

### Task 8: Streamlit Dashboard, README, And Final Verification

**Files:**
- Create: `dashboard/__init__.py`
- Create: `dashboard/app.py`
- Create: `tests/test_dashboard_data.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: gold Parquet files from Task 6 and Task 7.
- Produces: `GoldDataMissingError` and `GoldDataMalformedError` exceptions in `dashboard.app`.
- Produces: `load_gold_data(gold_dir: Path = Path("data/gold")) -> tuple[pd.DataFrame, pd.DataFrame]`.
- Produces: `render_dashboard(gold_dir: Path = Path("data/gold")) -> None`.
- Streamlit runs through `streamlit run dashboard/app.py`.

- [ ] **Step 1: Write failing dashboard data tests**

Create `tests/test_dashboard_data.py`:

```python
from pathlib import Path

import pandas as pd
import pytest

from dashboard.app import GoldDataMissingError, load_gold_data


def test_load_gold_data_reads_expected_files(tmp_path: Path) -> None:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    latest = pd.DataFrame([{"coin_id": "bitcoin", "latest_price": 58000.12}])
    overview = pd.DataFrame([{"coin_count": 1, "total_market_cap": 1140000000000}])
    latest.to_parquet(gold_dir / "gold_crypto_latest_prices.parquet", index=False)
    overview.to_parquet(gold_dir / "gold_market_overview.parquet", index=False)

    loaded_latest, loaded_overview = load_gold_data(gold_dir)

    assert loaded_latest.to_dict("records") == latest.to_dict("records")
    assert loaded_overview.to_dict("records") == overview.to_dict("records")


def test_load_gold_data_raises_clear_error_when_files_are_missing(tmp_path: Path) -> None:
    with pytest.raises(GoldDataMissingError, match="run the pipeline"):
        load_gold_data(tmp_path / "missing-gold")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_dashboard_data.py -v
```

Expected: FAIL during import with `ModuleNotFoundError: No module named 'dashboard'`.

- [ ] **Step 3: Implement dashboard app**

Create `dashboard/__init__.py`:

```python
"""Streamlit dashboard package for the crypto ETL MVP."""
```

Create `dashboard/app.py`:

```python
from pathlib import Path

import pandas as pd
import streamlit as st


LATEST_PRICES_FILE = "gold_crypto_latest_prices.parquet"
MARKET_OVERVIEW_FILE = "gold_market_overview.parquet"


class GoldDataMissingError(FileNotFoundError):
    pass


class GoldDataMalformedError(RuntimeError):
    pass


def load_gold_data(gold_dir: Path = Path("data/gold")) -> tuple[pd.DataFrame, pd.DataFrame]:
    latest_path = gold_dir / LATEST_PRICES_FILE
    overview_path = gold_dir / MARKET_OVERVIEW_FILE
    if not latest_path.exists() or not overview_path.exists():
        raise GoldDataMissingError(
            "Gold data is missing. Run the pipeline first with "
            "`python -m crypto_etl.orchestration.run_pipeline`, or run "
            "`python -m crypto_etl.orchestration.run_pipeline --use-sample-data` when offline."
        )
    try:
        return pd.read_parquet(latest_path), pd.read_parquet(overview_path)
    except Exception as exc:
        raise GoldDataMalformedError(f"Gold data could not be read: {exc}") from exc


def render_dashboard(gold_dir: Path = Path("data/gold")) -> None:
    st.set_page_config(page_title="Crypto Market Overview", layout="wide")
    st.title("Crypto Market Overview")

    try:
        latest, overview = load_gold_data(gold_dir)
    except GoldDataMissingError as exc:
        st.warning(str(exc))
        return
    except GoldDataMalformedError as exc:
        st.error(str(exc))
        return

    overview_row = overview.iloc[0]
    metric_columns = st.columns(5)
    metric_columns[0].metric(
        "Total Market Cap",
        f"{overview_row['total_market_cap']:,.0f}",
    )
    metric_columns[1].metric(
        "24h Volume",
        f"{overview_row['total_volume_24h']:,.0f}",
    )
    metric_columns[2].metric(
        "Avg 24h Return",
        f"{overview_row['average_return_24h_pct']:.2f}%",
    )
    metric_columns[3].metric(
        "Best Performer",
        str(overview_row["best_performer_coin_id"]),
    )
    metric_columns[4].metric("Tracked Coins", int(overview_row["coin_count"]))

    st.subheader("Latest Prices")
    st.dataframe(latest, use_container_width=True)

    st.subheader("Top Movers")
    movers = latest.sort_values("price_change_pct_24h", ascending=False).set_index(
        "coin_id"
    )
    st.bar_chart(movers["price_change_pct_24h"])

    st.subheader("Data Freshness")
    st.caption(f"Last updated: {overview_row['snapshot_timestamp_utc']}")


if __name__ == "__main__":
    render_dashboard()
```

- [ ] **Step 4: Update README quickstart**

Add this section after the README overview:

````markdown
## MVP Quickstart

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install the project with development dependencies:

```bash
pip install -e ".[dev]"
```

Run the test suite:

```bash
pytest
```

Run the pipeline with committed sample data when offline:

```bash
python -m crypto_etl.orchestration.run_pipeline --use-sample-data
```

Run the pipeline against CoinGecko:

```bash
python -m crypto_etl.orchestration.run_pipeline
```

Open the dashboard:

```bash
streamlit run dashboard/app.py
```
````

- [ ] **Step 5: Run dashboard tests**

Run:

```bash
pytest tests/test_dashboard_data.py -v
ruff check dashboard tests/test_dashboard_data.py
```

Expected: PASS for both commands.

- [ ] **Step 6: Run full verification**

Run:

```bash
pytest
ruff format --check .
ruff check .
python -m crypto_etl.orchestration.run_pipeline --use-sample-data
```

Expected: all commands exit 0. The final command logs `pipeline_succeeded` and writes ignored local files under `data/`.

- [ ] **Step 7: Commit task 8**

Run:

```bash
git add README.md dashboard tests/test_dashboard_data.py
git commit -m "feat(dashboard): add streamlit gold dashboard"
```

---

## Final Review Checklist

- [ ] Confirm `pytest` exits 0.
- [ ] Confirm `ruff format --check .` exits 0.
- [ ] Confirm `ruff check .` exits 0.
- [ ] Confirm `python -m crypto_etl.orchestration.run_pipeline --use-sample-data` exits 0.
- [ ] Confirm `data/raw/`, `data/bronze/`, `data/silver/`, and `data/gold/` are ignored by git.
- [ ] Confirm `dashboard/app.py` imports no CoinGecko client and reads only gold Parquet files.
- [ ] Confirm `git status --short` contains only intentional source, test, doc, or config changes before each commit.

## Implementation Order

Implement tasks in order. Do not start Task 3 before Task 2 passes and is committed, because each task relies on interfaces created by earlier tasks. If a task uncovers a mismatch in an earlier interface, stop, adjust the earlier interface deliberately, rerun all affected tests, and include the interface correction in the current task commit.
