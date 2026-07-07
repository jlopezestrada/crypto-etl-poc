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
