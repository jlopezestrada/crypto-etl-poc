from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import pandas as pd

Severity = Literal["critical", "warning"]


@dataclass(frozen=True)
class QualityIssue:
    severity: Severity
    check_name: str
    message: str


class QualityCheckError(RuntimeError):
    pass


def _issue(severity: Severity, check_name: str, message: str) -> QualityIssue:
    return QualityIssue(severity=severity, check_name=check_name, message=message)


def run_silver_quality_checks(
    df: pd.DataFrame,
    configured_coins: Sequence[str],
) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    if df.empty:
        return [_issue("critical", "silver_not_empty", "silver dataset must not be empty")]

    if df["coin_id"].isna().any():
        issues.append(_issue("critical", "coin_id_not_null", "coin_id must not be null"))
    if df["symbol"].isna().any():
        issues.append(_issue("critical", "symbol_not_null", "symbol must not be null"))

    for column, check_name in [
        ("price", "price_non_negative"),
        ("market_cap", "market_cap_non_negative"),
        ("volume_24h", "volume_24h_non_negative"),
    ]:
        if (df[column] < 0).any():
            issues.append(_issue("critical", check_name, f"{column} must not be negative"))

    if df.duplicated(subset=["coin_id", "extracted_at_utc"]).any():
        issues.append(
            _issue(
                "critical",
                "duplicate_coin_extraction_timestamp",
                "silver must not contain duplicate coin_id and extracted_at_utc rows",
            )
        )

    current_time = pd.Timestamp(datetime.now(UTC))
    if (pd.to_datetime(df["last_updated_utc"], utc=True) > current_time).any():
        issues.append(
            _issue(
                "critical",
                "last_updated_not_in_future",
                "last_updated_utc must not be in the future",
            )
        )

    missing = sorted(set(configured_coins) - set(df["coin_id"].dropna()))
    if missing:
        issues.append(
            _issue(
                "warning",
                "configured_coins_present",
                f"configured coins missing from data: {', '.join(missing)}",
            )
        )

    return issues


def run_gold_quality_checks(
    latest_df: pd.DataFrame,
    overview_df: pd.DataFrame,
    configured_coins: Sequence[str],
) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    if latest_df.empty:
        issues.append(
            _issue("critical", "latest_prices_not_empty", "latest prices must not be empty")
        )
        return issues

    if latest_df["coin_id"].duplicated().any():
        issues.append(
            _issue(
                "critical",
                "latest_prices_one_row_per_coin",
                "latest prices must contain one row per coin",
            )
        )

    if not overview_df.empty:
        available_count = len(set(latest_df["coin_id"].dropna()))
        overview_count = int(overview_df.iloc[0]["coin_count"])
        if overview_count != available_count:
            issues.append(
                _issue(
                    "warning",
                    "overview_coin_count_matches_latest",
                    "overview coin_count differs from latest prices coin count",
                )
            )

    missing = sorted(set(configured_coins) - set(latest_df["coin_id"].dropna()))
    if missing:
        issues.append(
            _issue(
                "warning",
                "gold_configured_coins_present",
                f"configured coins missing from gold data: {', '.join(missing)}",
            )
        )

    return issues


def raise_for_critical_issues(issues: Sequence[QualityIssue]) -> None:
    critical = [issue for issue in issues if issue.severity == "critical"]
    if critical:
        details = "; ".join(f"{issue.check_name}: {issue.message}" for issue in critical)
        raise QualityCheckError(details)
