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
