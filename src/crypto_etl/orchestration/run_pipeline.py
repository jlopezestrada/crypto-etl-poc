import argparse
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from crypto_etl.clients.coingecko_client import fetch_coin_markets
from crypto_etl.config import apply_overrides, load_config
from crypto_etl.load.raw_loader import build_raw_envelope, load_sample_fixture, write_raw_response
from crypto_etl.logging_config import configure_logging
from crypto_etl.quality.checks import (
    QualityIssue,
    raise_for_critical_issues,
    run_gold_quality_checks,
    run_silver_quality_checks,
)
from crypto_etl.transform.bronze_transform import transform_raw_to_bronze, write_bronze_snapshot
from crypto_etl.transform.gold_transform import (
    build_latest_prices,
    build_market_overview,
    write_gold_dataset,
)
from crypto_etl.transform.silver_transform import transform_bronze_to_silver, write_silver_snapshot
from crypto_etl.utils.time import make_run_id, utc_now

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    raw_path: Path
    bronze_path: Path
    silver_path: Path
    latest_prices_path: Path
    market_overview_path: Path
    quality_issues: list[QualityIssue]


def run_pipeline(
    config_path: Path = Path("config/config.yaml"),
    coins_override: list[str] | None = None,
    currency_override: str | None = None,
    use_sample_data: bool = False,
    sample_path: Path = Path("tests/fixtures/coingecko_market_sample.json"),
) -> PipelineResult:
    started_at = time.perf_counter()
    try:
        config = apply_overrides(load_config(config_path), coins_override, currency_override)
        requested_at = utc_now()
        run_id = make_run_id(requested_at)
        LOGGER.info(
            "pipeline_started run_id=%s coins=%s currency=%s",
            run_id,
            config.coins,
            config.currency,
        )

        if use_sample_data:
            sample = load_sample_fixture(sample_path)
            response = sample["response"]
            params = sample["params"]
            params["ids"] = config.coins
            params["vs_currency"] = config.currency
        else:
            response, params = fetch_coin_markets(config.coins, config.currency, config.api)

        envelope = build_raw_envelope(
            provider="coingecko",
            endpoint="coins_markets",
            run_id=run_id,
            requested_at_utc=requested_at,
            params=params,
            response=response,
        )
        raw_path = write_raw_response(envelope, config.paths.raw_dir)
        LOGGER.info("raw_written path=%s records=%s", raw_path, len(response))

        if not response:
            raise RuntimeError("extraction returned zero records")

        bronze = transform_raw_to_bronze(envelope, raw_path)
        bronze_path = write_bronze_snapshot(bronze, config.paths.bronze_dir)
        LOGGER.info("bronze_written path=%s records=%s", bronze_path, len(bronze))

        silver = transform_bronze_to_silver(bronze, config.currency)
        silver_path = write_silver_snapshot(silver, config.paths.silver_dir)
        silver_issues = run_silver_quality_checks(silver, config.coins)
        raise_for_critical_issues(silver_issues)
        LOGGER.info(
            "silver_written path=%s records=%s issues=%s",
            silver_path,
            len(silver),
            len(silver_issues),
        )

        latest = build_latest_prices(silver)
        overview = build_market_overview(latest)
        latest_path = write_gold_dataset(latest, config.paths.gold_dir, "gold_crypto_latest_prices")
        overview_path = write_gold_dataset(overview, config.paths.gold_dir, "gold_market_overview")
        gold_issues = run_gold_quality_checks(latest, overview, config.coins)
        raise_for_critical_issues(gold_issues)
        LOGGER.info(
            "gold_written latest=%s overview=%s issues=%s",
            latest_path,
            overview_path,
            len(gold_issues),
        )

        quality_issues = silver_issues + gold_issues
        for issue in quality_issues:
            if issue.severity == "warning":
                LOGGER.warning(
                    "quality_issue_warning check_name=%s message=%s",
                    issue.check_name,
                    issue.message,
                )

        duration_seconds = time.perf_counter() - started_at
        LOGGER.info("pipeline_succeeded run_id=%s duration_seconds=%.3f", run_id, duration_seconds)

        return PipelineResult(
            run_id=run_id,
            raw_path=raw_path,
            bronze_path=bronze_path,
            silver_path=silver_path,
            latest_prices_path=latest_path,
            market_overview_path=overview_path,
            quality_issues=quality_issues,
        )
    except Exception as exc:
        duration_seconds = time.perf_counter() - started_at
        LOGGER.error("pipeline_failed duration_seconds=%.3f error=%s", duration_seconds, exc)
        raise


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the crypto ETL pipeline")
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    parser.add_argument("--coins", type=str, default=None)
    parser.add_argument("--currency", type=str, default=None)
    parser.add_argument("--use-sample-data", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    coins_override = args.coins.split(",") if args.coins else None
    run_pipeline(
        config_path=args.config,
        coins_override=coins_override,
        currency_override=args.currency,
        use_sample_data=args.use_sample_data,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
