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
