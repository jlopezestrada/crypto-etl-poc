from pathlib import Path

import pandas as pd
import pytest

from dashboard.app import GoldDataMissingError, load_gold_data


def test_load_gold_data_reads_expected_files(tmp_path: Path) -> None:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    latest = pd.DataFrame([{"coin_id": "bitcoin", "latest_price": 58000.12}])
    overview = pd.DataFrame([{"coin_count": 1, "total_market_cap": 1140000000000}])
    latest.to_parquet(gold_dir / "gold_crypto_latest_prices.parquet", index=False)
    overview.to_parquet(gold_dir / "gold_market_overview.parquet", index=False)

    loaded_latest, loaded_overview = load_gold_data(gold_dir)

    assert loaded_latest.to_dict("records") == latest.to_dict("records")
    assert loaded_overview.to_dict("records") == overview.to_dict("records")


def test_load_gold_data_raises_clear_error_when_files_are_missing(tmp_path: Path) -> None:
    with pytest.raises(GoldDataMissingError, match="run the pipeline"):
        load_gold_data(tmp_path / "missing-gold")
