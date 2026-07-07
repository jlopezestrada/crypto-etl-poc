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
