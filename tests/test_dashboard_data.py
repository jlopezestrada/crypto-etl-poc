import inspect
from pathlib import Path

import pandas as pd
import pytest

from dashboard.app import (
    GoldDataMalformedError,
    GoldDataMissingError,
    load_gold_data,
    render_dashboard,
)


def write_gold_data(gold_dir: Path, latest: pd.DataFrame, overview: pd.DataFrame) -> None:
    gold_dir.mkdir()
    latest.to_parquet(gold_dir / "gold_crypto_latest_prices.parquet", index=False)
    overview.to_parquet(gold_dir / "gold_market_overview.parquet", index=False)


def valid_latest() -> pd.DataFrame:
    return pd.DataFrame(
        [{"coin_id": "bitcoin", "latest_price": 58000.12, "price_change_pct_24h": 1.58}]
    )


def valid_overview() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "snapshot_timestamp_utc": "2026-07-06T10:29:00+00:00",
                "total_market_cap": 1140000000000,
                "total_volume_24h": 32000000000,
                "average_return_24h_pct": 1.58,
                "best_performer_coin_id": "bitcoin",
                "worst_performer_coin_id": "bitcoin",
                "coin_count": 1,
            }
        ]
    )


def test_load_gold_data_reads_expected_files(tmp_path: Path) -> None:
    gold_dir = tmp_path / "gold"
    latest = valid_latest()
    overview = valid_overview()
    write_gold_data(gold_dir, latest, overview)

    loaded_latest, loaded_overview = load_gold_data(gold_dir)

    assert loaded_latest.to_dict("records") == latest.to_dict("records")
    assert loaded_overview.to_dict("records") == overview.to_dict("records")


def test_load_gold_data_raises_clear_error_when_files_are_missing(tmp_path: Path) -> None:
    with pytest.raises(GoldDataMissingError, match="run the pipeline"):
        load_gold_data(tmp_path / "missing-gold")


@pytest.mark.parametrize(
    ("latest", "overview", "match"),
    [
        (
            pd.DataFrame(columns=valid_latest().columns),
            valid_overview(),
            "latest prices gold data must not be empty",
        ),
        (
            valid_latest(),
            pd.DataFrame(columns=valid_overview().columns),
            "market overview gold data must not be empty",
        ),
    ],
)
def test_load_gold_data_raises_clear_error_when_gold_data_is_empty(
    tmp_path: Path, latest: pd.DataFrame, overview: pd.DataFrame, match: str
) -> None:
    gold_dir = tmp_path / "gold"
    write_gold_data(gold_dir, latest, overview)

    with pytest.raises(GoldDataMalformedError, match=match):
        load_gold_data(gold_dir)


@pytest.mark.parametrize(
    ("latest", "overview", "match"),
    [
        (
            pd.DataFrame([{"coin_id": "bitcoin", "latest_price": 58000.12}]),
            valid_overview(),
            "latest prices gold data is missing required columns: price_change_pct_24h",
        ),
        (
            valid_latest(),
            valid_overview().drop(columns=["worst_performer_coin_id"]),
            "market overview gold data is missing required columns: worst_performer_coin_id",
        ),
    ],
)
def test_load_gold_data_raises_clear_error_when_required_columns_are_missing(
    tmp_path: Path, latest: pd.DataFrame, overview: pd.DataFrame, match: str
) -> None:
    gold_dir = tmp_path / "gold"
    write_gold_data(gold_dir, latest, overview)

    with pytest.raises(GoldDataMalformedError, match=match):
        load_gold_data(gold_dir)


def test_dashboard_renders_worst_performer_metric() -> None:
    source = inspect.getsource(render_dashboard)

    assert "Worst Performer" in source
    assert "worst_performer_coin_id" in source
