import json
from datetime import datetime, timedelta
from fastapi import HTTPException


def parse_ts(value):
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {value}") from exc


def full_hour_range(hours):
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    return now - timedelta(hours=hours), now


def row_to_dict(row):
    result = {}
    for key, value in row._asdict().items():
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
        elif key in {"top_authors", "top_bots", "top_humans", "active_hours"} and isinstance(value, str):
            try:
                result[key] = json.loads(value)
            except Exception:
                result[key] = value
        else:
            result[key] = value
    return result
