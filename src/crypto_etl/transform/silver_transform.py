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

    silver = silver.drop_duplicates(subset=["coin_id", "extracted_at_utc"], keep="first")
    return silver.reindex(columns=SILVER_COLUMNS)


def write_silver_snapshot(df: pd.DataFrame, silver_dir: Path) -> Path:
    output_path = silver_dir / "crypto_market_snapshot" / "data.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    return output_path
