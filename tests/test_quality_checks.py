from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from crypto_etl.quality.checks import (
    QualityCheckError,
    raise_for_critical_issues,
    run_gold_quality_checks,
    run_silver_quality_checks,
)


def valid_silver_frame() -> pd.DataFrame:
    extracted_at = pd.Timestamp(datetime(2026, 7, 6, 10, 30, tzinfo=UTC))
    return pd.DataFrame(
        [
            {
                "coin_id": "bitcoin",
                "symbol": "BTC",
                "price": 58000.12,
                "market_cap": 1140000000000,
                "volume_24h": 32000000000,
                "extracted_at_utc": extracted_at,
                "last_updated_utc": extracted_at,
            },
            {
                "coin_id": "ethereum",
                "symbol": "ETH",
                "price": 3100.5,
                "market_cap": 372000000000,
                "volume_24h": 18000000000,
                "extracted_at_utc": extracted_at,
                "last_updated_utc": extracted_at,
            },
        ]
    )


def test_silver_quality_checks_return_no_issues_for_valid_data() -> None:
    issues = run_silver_quality_checks(valid_silver_frame(), ["bitcoin", "ethereum"])

    assert issues == []


def test_silver_quality_checks_detect_critical_failures() -> None:
    frame = valid_silver_frame()
    frame.loc[0, "coin_id"] = None
    frame.loc[1, "price"] = -1

    issues = run_silver_quality_checks(frame, ["bitcoin", "ethereum"])

    assert {issue.check_name for issue in issues if issue.severity == "critical"} == {
        "coin_id_not_null",
        "price_non_negative",
    }


def test_silver_quality_checks_detect_duplicates_and_future_timestamps() -> None:
    frame = pd.concat([valid_silver_frame(), valid_silver_frame().iloc[[0]]], ignore_index=True)
    frame.loc[0, "last_updated_utc"] = pd.Timestamp(datetime.now(UTC) + timedelta(days=1))

    issues = run_silver_quality_checks(frame, ["bitcoin", "ethereum"])

    assert {issue.check_name for issue in issues if issue.severity == "critical"} == {
        "duplicate_coin_extraction_timestamp",
        "last_updated_not_in_future",
    }


def test_silver_quality_checks_warn_for_missing_configured_coin() -> None:
    issues = run_silver_quality_checks(valid_silver_frame(), ["bitcoin", "ethereum", "solana"])

    assert [(issue.severity, issue.check_name) for issue in issues] == [
        ("warning", "configured_coins_present")
    ]


def test_gold_quality_checks_latest_prices_one_row_per_coin() -> None:
    latest = pd.DataFrame(
        [
            {"coin_id": "bitcoin", "latest_price": 58000.12},
            {"coin_id": "bitcoin", "latest_price": 58000.12},
        ]
    )
    overview = pd.DataFrame([{"coin_count": 1}])

    issues = run_gold_quality_checks(latest, overview, ["bitcoin"])

    assert [(issue.severity, issue.check_name) for issue in issues] == [
        ("critical", "latest_prices_one_row_per_coin")
    ]


def test_raise_for_critical_issues_raises_for_critical_issue() -> None:
    issues = run_silver_quality_checks(pd.DataFrame(), ["bitcoin"])

    with pytest.raises(QualityCheckError, match="silver_not_empty"):
        raise_for_critical_issues(issues)
