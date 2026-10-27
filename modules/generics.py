import time
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist() -> datetime:
    return datetime.now(IST)


def current_date_ist() -> str:
    return now_ist().strftime("%B %d, %Y")


def now_iso() -> str:
    return now_ist().isoformat()


def get_timestamp() -> float:
    return time.time()


def format_timestamp(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=IST)
    return dt.strftime("%H:%M:%S %d:%m:%Y")


def parse_datetime(dt_str: str) -> float:
    dt = datetime.fromisoformat(dt_str)
    return dt.timestamp()


def utc_to_local(dt_str: str | None = None) -> str:
    if dt_str is None:
        return now_ist().isoformat()
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(IST).isoformat()