import logging
import time
from collections.abc import Sequence
from typing import Any

import httpx

from crypto_etl.config import ApiConfig

LOGGER = logging.getLogger(__name__)


def fetch_coin_markets(
    coins: Sequence[str],
    currency: str,
    api_config: ApiConfig,
    transport: httpx.BaseTransport | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: dict[str, Any] = {
        "vs_currency": currency,
        "ids": list(coins),
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": False,
    }
    request_params = {**params, "ids": ",".join(coins)}

    last_error: Exception | None = None
    with httpx.Client(
        base_url=api_config.base_url,
        timeout=api_config.timeout_seconds,
        transport=transport,
    ) as client:
        for attempt in range(1, api_config.max_retries + 1):
            try:
                response = client.get("coins/markets", params=request_params)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("CoinGecko /coins/markets response must be a list")
                return payload, params
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                LOGGER.warning("coingecko_request_failed attempt=%s error=%s", attempt, exc)
                if attempt == api_config.max_retries:
                    break
                time.sleep(api_config.retry_backoff_seconds * attempt)

    raise RuntimeError("CoinGecko market data request failed") from last_error
