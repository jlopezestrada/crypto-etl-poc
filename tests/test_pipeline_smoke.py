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
