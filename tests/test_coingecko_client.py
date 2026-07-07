import httpx

from crypto_etl.clients.coingecko_client import fetch_coin_markets
from crypto_etl.config import ApiConfig


def test_fetch_coin_markets_builds_expected_request() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[{"id": "bitcoin"}])

    response, params = fetch_coin_markets(
        coins=["bitcoin", "ethereum"],
        currency="eur",
        api_config=ApiConfig(timeout_seconds=1, max_retries=1, retry_backoff_seconds=0.01),
        transport=httpx.MockTransport(handler),
    )

    assert response == [{"id": "bitcoin"}]
    assert params["ids"] == ["bitcoin", "ethereum"]
    assert requests[0].url.path == "/api/v3/coins/markets"
    assert requests[0].url.params["vs_currency"] == "eur"
    assert requests[0].url.params["ids"] == "bitcoin,ethereum"


def test_fetch_coin_markets_retries_temporary_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, json={"error": "rate limited"})
        return httpx.Response(200, json=[{"id": "bitcoin"}])

    response, _ = fetch_coin_markets(
        coins=["bitcoin"],
        currency="eur",
        api_config=ApiConfig(timeout_seconds=1, max_retries=2, retry_backoff_seconds=0.01),
        transport=httpx.MockTransport(handler),
    )

    assert response == [{"id": "bitcoin"}]
    assert calls == 2
