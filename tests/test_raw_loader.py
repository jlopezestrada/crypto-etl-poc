from datetime import UTC, datetime
from pathlib import Path

from crypto_etl.load.raw_loader import (
    build_raw_envelope,
    load_sample_fixture,
    read_raw_response,
    write_raw_response,
)


def test_build_raw_envelope_preserves_response() -> None:
    response = [{"id": "bitcoin", "current_price": 58000.12}]
    requested_at = datetime(2026, 7, 6, 10, 30, tzinfo=UTC)

    envelope = build_raw_envelope(
        provider="coingecko",
        endpoint="coins_markets",
        run_id="run-1",
        requested_at_utc=requested_at,
        params={"vs_currency": "eur", "ids": ["bitcoin"]},
        response=response,
    )

    assert envelope["provider"] == "coingecko"
    assert envelope["endpoint"] == "coins_markets"
    assert envelope["run_id"] == "run-1"
    assert envelope["requested_at_utc"] == "2026-07-06T10:30:00+00:00"
    assert envelope["params"] == {"vs_currency": "eur", "ids": ["bitcoin"]}
    assert envelope["response"] is response


def test_write_raw_response_partitions_by_requested_date(tmp_path: Path) -> None:
    envelope = build_raw_envelope(
        provider="coingecko",
        endpoint="coins_markets",
        run_id="run-1",
        requested_at_utc=datetime(2026, 7, 6, 10, 30, tzinfo=UTC),
        params={"vs_currency": "eur"},
        response=[{"id": "bitcoin"}],
    )

    output_path = write_raw_response(envelope, tmp_path)

    expected_path = (
        tmp_path
        / "coingecko"
        / "market_data"
        / "year=2026"
        / "month=07"
        / "day=06"
        / "run_id=run-1"
        / "response.json"
    )
    assert output_path == expected_path
    assert read_raw_response(output_path) == envelope


def test_load_sample_fixture_reads_committed_envelope() -> None:
    sample = load_sample_fixture()

    assert sample["provider"] == "coingecko"
    assert sample["endpoint"] == "coins_markets"
    assert [coin["id"] for coin in sample["response"]] == ["bitcoin", "ethereum", "solana"]
