from datetime import UTC, datetime
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


def make_run_id(now: datetime | None = None) -> str:
    timestamp = now or utc_now()
    return f"{timestamp:%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
