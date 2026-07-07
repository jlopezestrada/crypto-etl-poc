from pathlib import Path

import pandas as pd
import streamlit as st

LATEST_PRICES_FILE = "gold_crypto_latest_prices.parquet"
MARKET_OVERVIEW_FILE = "gold_market_overview.parquet"


class GoldDataMissingError(FileNotFoundError):
    pass


class GoldDataMalformedError(RuntimeError):
    pass


def load_gold_data(gold_dir: Path = Path("data/gold")) -> tuple[pd.DataFrame, pd.DataFrame]:
    latest_path = gold_dir / LATEST_PRICES_FILE
    overview_path = gold_dir / MARKET_OVERVIEW_FILE
    if not latest_path.exists() or not overview_path.exists():
        raise GoldDataMissingError(
            "Gold data is missing. run the pipeline first with "
            "`python -m crypto_etl.orchestration.run_pipeline`, or run "
            "`python -m crypto_etl.orchestration.run_pipeline --use-sample-data` when offline."
        )
    try:
        return pd.read_parquet(latest_path), pd.read_parquet(overview_path)
    except Exception as exc:
        raise GoldDataMalformedError(f"Gold data could not be read: {exc}") from exc


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
    metric_columns = st.columns(5)
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
    metric_columns[4].metric("Tracked Coins", int(overview_row["coin_count"]))

    st.subheader("Latest Prices")
    st.dataframe(latest, use_container_width=True)

    st.subheader("Top Movers")
    movers = latest.sort_values("price_change_pct_24h", ascending=False).set_index("coin_id")
    st.bar_chart(movers["price_change_pct_24h"])

    st.subheader("Data Freshness")
    st.caption(f"Last updated: {overview_row['snapshot_timestamp_utc']}")


if __name__ == "__main__":
    render_dashboard()
