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
