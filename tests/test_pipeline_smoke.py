import logging
from pathlib import Path

import pandas as pd
import pytest

from crypto_etl.orchestration.run_pipeline import run_pipeline


def write_smoke_config(tmp_path: Path, coins: list[str] | None = None) -> Path:
    config_path = tmp_path / "config.yaml"
    configured_coins = coins or ["bitcoin", "ethereum", "solana"]
    coin_lines = "\n".join(f"  - {coin}" for coin in configured_coins)
    config_path.write_text(
        f"""
coins:
{coin_lines}
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


def test_run_pipeline_with_sample_data_writes_all_layers(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="crypto_etl.orchestration.run_pipeline"):
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

    success_logs = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("pipeline_succeeded ")
    ]
    assert len(success_logs) == 1
    assert "duration_seconds=" in success_logs[0]


def test_run_pipeline_logs_warning_quality_issues_for_missing_configured_coins(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_path = write_smoke_config(tmp_path, coins=["bitcoin", "ethereum", "solana", "dogecoin"])

    with caplog.at_level(logging.WARNING, logger="crypto_etl.orchestration.run_pipeline"):
        result = run_pipeline(config_path=config_path, use_sample_data=True)

    assert [issue.check_name for issue in result.quality_issues] == [
        "configured_coins_present",
        "gold_configured_coins_present",
    ]
    warning_logs = [record.getMessage() for record in caplog.records]
    assert (
        "quality_issue_warning check_name=configured_coins_present "
        "message=configured coins missing from data: dogecoin"
    ) in warning_logs
    assert (
        "quality_issue_warning check_name=gold_configured_coins_present "
        "message=configured coins missing from gold data: dogecoin"
    ) in warning_logs


def test_run_pipeline_logs_failure_with_duration(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with (
        caplog.at_level(logging.ERROR, logger="crypto_etl.orchestration.run_pipeline"),
        pytest.raises(FileNotFoundError),
    ):
        run_pipeline(config_path=tmp_path / "missing-config.yaml", use_sample_data=True)

    failure_logs = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("pipeline_failed ")
    ]
    assert len(failure_logs) == 1
    assert "duration_seconds=" in failure_logs[0]
    assert "error=" in failure_logs[0]
