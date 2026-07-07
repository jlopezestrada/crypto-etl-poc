from collections.abc import Mapping
from pathlib import Path
from typing import Any

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
