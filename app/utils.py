from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional


SEVERITY_MAP = {
    "1": "low",
    "2": "moderate",
    "3": "considerable",
    "4": "high",
    "5": "extreme",
    "low": "low",
    "moderate": "moderate",
    "considerable": "considerable",
    "high": "high",
    "extreme": "extreme",
    "no rating": "unknown",
    "none": "unknown",
}


def normalize_severity(value: Optional[str]) -> str:
    if value is None:
        return "unknown"
    normalized = str(value).strip().lower()
    return SEVERITY_MAP.get(normalized, "unknown")


def parse_timestamp(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
            return dt.isoformat()
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        value = value.strip()
        formats = [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                continue
    return None


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def extract_coordinates(payload: dict) -> tuple[Optional[float], Optional[float]]:
    lat = payload.get("lat") or payload.get("latitude")
    lon = payload.get("lon") or payload.get("longitude")
    if lat is None and lon is None:
        loc = payload.get("location") or payload.get("coordinates")
        if isinstance(loc, dict):
            lat = loc.get("lat") or loc.get("latitude")
            lon = loc.get("lon") or loc.get("longitude")
    try:
        lat = float(lat) if lat is not None else None
    except (ValueError, TypeError):
        lat = None
    try:
        lon = float(lon) if lon is not None else None
    except (ValueError, TypeError):
        lon = None
    return lat, lon


def validate_metrics(metrics: dict) -> tuple[dict, list]:
    flags = []
    cleaned = {}
    for k, v in metrics.items():
        if not isinstance(v, (int, float, str, bool, type(None))):
            flags.append("invalid_metric_type")
        else:
            cleaned[k] = v
    return cleaned, flags
