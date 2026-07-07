from datetime import UTC, datetime

from crypto_etl.utils.time import make_run_id, utc_now


def test_utc_now_returns_timezone_aware_utc_datetime() -> None:
    current = utc_now()

    assert current.tzinfo == UTC


def test_make_run_id_uses_timestamp_and_suffix() -> None:
    run_id = make_run_id(datetime(2026, 7, 6, 10, 30, 0, tzinfo=UTC))

    assert run_id.startswith("20260706_103000_")
    assert len(run_id) > len("20260706_103000_")
