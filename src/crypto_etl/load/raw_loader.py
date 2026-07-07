import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

SAMPLE_FIXTURE_PATH = Path("tests/fixtures/coingecko_market_sample.json")


def build_raw_envelope(
    provider: str,
    endpoint: str,
    run_id: str,
    requested_at_utc: datetime,
    params: Mapping[str, Any],
    response: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "provider": provider,
        "endpoint": endpoint,
        "run_id": run_id,
        "requested_at_utc": requested_at_utc.isoformat(),
        "params": dict(params),
        "response": response,
    }


def write_raw_response(envelope: Mapping[str, Any], raw_dir: Path) -> Path:
    requested_at = datetime.fromisoformat(str(envelope["requested_at_utc"]))
    output_path = (
        raw_dir
        / str(envelope["provider"])
        / "market_data"
        / f"year={requested_at:%Y}"
        / f"month={requested_at:%m}"
        / f"day={requested_at:%d}"
        / f"run_id={envelope['run_id']}"
        / "response.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
    return output_path


def read_raw_response(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_sample_fixture(path: Path = SAMPLE_FIXTURE_PATH) -> dict[str, Any]:
    return read_raw_response(path)
