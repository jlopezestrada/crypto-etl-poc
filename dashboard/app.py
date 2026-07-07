from pathlib import Path

import pandas as pd
import streamlit as st

LATEST_PRICES_FILE = "gold_crypto_latest_prices.parquet"
MARKET_OVERVIEW_FILE = "gold_market_overview.parquet"
LATEST_PRICES_REQUIRED_COLUMNS = {"coin_id", "price_change_pct_24h"}
LATEST_PRICES_NUMERIC_COLUMNS = ("price_change_pct_24h",)
MARKET_OVERVIEW_REQUIRED_COLUMNS = {
    "snapshot_timestamp_utc",
    "total_market_cap",
    "total_volume_24h",
    "average_return_24h_pct",
    "best_performer_coin_id",
    "worst_performer_coin_id",
    "coin_count",
}
MARKET_OVERVIEW_NUMERIC_COLUMNS = (
    "total_market_cap",
    "total_volume_24h",
    "average_return_24h_pct",
    "coin_count",
)


class GoldDataMissingError(FileNotFoundError):
    pass


class GoldDataMalformedError(RuntimeError):
    pass


def validate_gold_frame(frame: pd.DataFrame, label: str, required_columns: set[str]) -> None:
    if frame.empty:
        raise GoldDataMalformedError(f"{label} gold data must not be empty")
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise GoldDataMalformedError(
            f"{label} gold data is missing required columns: {', '.join(missing_columns)}"
        )


def coerce_numeric_columns(
    frame: pd.DataFrame, label: str, columns: tuple[str, ...]
) -> pd.DataFrame:
    coerced = frame.copy()
    malformed_columns = []
    for column in columns:
        numeric_values = pd.to_numeric(coerced[column], errors="coerce")
        if numeric_values.isna().any():
            malformed_columns.append(column)
        else:
            coerced[column] = numeric_values
    if malformed_columns:
        raise GoldDataMalformedError(
            f"{label} gold data has non-numeric or null values in columns: "
            f"{', '.join(malformed_columns)}"
        )
    return coerced


def load_gold_data(gold_dir: Path = Path("data/gold")) -> tuple[pd.DataFrame, pd.DataFrame]:
    latest_path = gold_dir / LATEST_PRICES_FILE
    overview_path = gold_dir / MARKET_OVERVIEW_FILE
    if not latest_path.exists() or not overview_path.exists():
        raise GoldDataMissingError(
            "Gold data is missing; run the pipeline first with "
            "`python -m crypto_etl.orchestration.run_pipeline`, or run "
            "`python -m crypto_etl.orchestration.run_pipeline --use-sample-data` when offline."
        )
    try:
        latest = pd.read_parquet(latest_path)
        overview = pd.read_parquet(overview_path)
    except Exception as exc:
        raise GoldDataMalformedError(f"Gold data could not be read: {exc}") from exc
    validate_gold_frame(latest, "latest prices", LATEST_PRICES_REQUIRED_COLUMNS)
    validate_gold_frame(overview, "market overview", MARKET_OVERVIEW_REQUIRED_COLUMNS)
    latest = coerce_numeric_columns(latest, "latest prices", LATEST_PRICES_NUMERIC_COLUMNS)
    overview = coerce_numeric_columns(overview, "market overview", MARKET_OVERVIEW_NUMERIC_COLUMNS)
    return latest, overview


def render_dashboard(gold_dir: Path = Path("data/gold")) -> None:
    st.set_page_config(page_title="Crypto Market Overview", layout="wide")
    st.title("Crypto Market Overview")

    try:
        latest, overview = load_gold_data(gold_dir)
    except GoldDataMissingError as exc:
        st.warning(str(exc))
        return
    except GoldDataMalformedError as exc:
        st.error(str(exc))
        return

    overview_row = overview.iloc[0]
    metric_columns = st.columns(6)
    metric_columns[0].metric(
        "Total Market Cap",
        f"{overview_row['total_market_cap']:,.0f}",
    )
    metric_columns[1].metric(
        "24h Volume",
        f"{overview_row['total_volume_24h']:,.0f}",
    )
    metric_columns[2].metric(
        "Avg 24h Return",
        f"{overview_row['average_return_24h_pct']:.2f}%",
    )
    metric_columns[3].metric(
        "Best Performer",
        str(overview_row["best_performer_coin_id"]),
    )
    metric_columns[4].metric(
        "Worst Performer",
        str(overview_row["worst_performer_coin_id"]),
    )
    metric_columns[5].metric("Tracked Coins", int(overview_row["coin_count"]))

    st.subheader("Latest Prices")
    st.dataframe(latest, use_container_width=True)

    st.subheader("Top Movers")
    movers = latest.sort_values("price_change_pct_24h", ascending=False).set_index("coin_id")
    st.bar_chart(movers["price_change_pct_24h"])

    st.subheader("Data Freshness")
    st.caption(f"Last updated: {overview_row['snapshot_timestamp_utc']}")


if __name__ == "__main__":
    render_dashboard()
