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
