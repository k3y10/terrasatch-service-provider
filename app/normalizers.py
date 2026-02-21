"""
Core ingestion and normalization logic for TerraSatch.

Each normalizer accepts a raw payload dict and provider metadata dict,
and returns a partial NormalizedRecord dict (excluding record_id, ingested_at
and raw_payload which are filled in by the dispatcher).
"""
from __future__ import annotations
from typing import Any, Dict

from app.utils import (
    extract_coordinates,
    normalize_severity,
    now_iso,
    parse_timestamp,
    safe_str,
    validate_metrics,
)


ProviderMeta = Dict[str, Any]


def _build_location(payload: dict) -> dict:
    lat, lon = extract_coordinates(payload)
    loc_block = payload.get("location") or payload.get("coordinates") or {}
    loc_name = None
    elevation_ft = None
    if isinstance(loc_block, dict):
        loc_name = safe_str(loc_block.get("name")) or None
        elevation_ft = loc_block.get("elevation_ft") or loc_block.get("elevation")
    loc_name = loc_name or safe_str(payload.get("location_name")) or None
    if elevation_ft is None:
        elevation_ft = payload.get("elevation_ft") or payload.get("elevation")
    try:
        elevation_ft = int(elevation_ft) if elevation_ft is not None else None
    except (ValueError, TypeError):
        elevation_ft = None
    return {"name": loc_name, "lat": lat, "lon": lon, "elevation_ft": elevation_ft}


def _collect_quality_flags(record: dict) -> list:
    flags = []
    loc = record.get("location", {})
    if loc.get("lat") is None or loc.get("lon") is None:
        flags.append("missing_coordinates")
    if not record.get("event_time"):
        flags.append("missing_timestamp")
    if record.get("severity") == "unknown":
        flags.append("unknown_severity")
    if not record.get("summary", "").strip():
        flags.append("empty_summary")
    return flags


def normalize_bulletin(payload: dict, provider_meta: ProviderMeta) -> dict:
    """Normalize a forecast bulletin payload into TerraSatch format."""
    raw_severity = (
        payload.get("danger")
        or payload.get("avalanche_danger")
        or payload.get("severity")
        or payload.get("danger_level")
    )
    raw_time = (
        payload.get("issued_at")
        or payload.get("timestamp")
        or payload.get("valid_time")
        or payload.get("date")
    )
    summary = safe_str(
        payload.get("summary")
        or payload.get("headline")
        or payload.get("description")
        or payload.get("text")
        or ""
    )
    region = safe_str(
        payload.get("region")
        or payload.get("zone")
        or payload.get("area")
        or ""
    ) or None

    raw_metrics = payload.get("metrics") or {}
    if not isinstance(raw_metrics, dict):
        raw_metrics = {}
    for key in ("rose", "aspects", "elevation_bands", "travel_advice"):
        val = payload.get(key)
        if val is not None:
            raw_metrics[key] = val
    metrics, metric_flags = validate_metrics(raw_metrics)

    record = {
        "provider_id": safe_str(provider_meta.get("provider_id")),
        "provider_name": safe_str(provider_meta.get("provider_name")),
        "record_type": "bulletin",
        "region": region,
        "location": _build_location(payload),
        "event_time": parse_timestamp(raw_time),
        "severity": normalize_severity(str(raw_severity) if raw_severity is not None else None),
        "metrics": metrics,
        "summary": summary,
    }
    flags = _collect_quality_flags(record) + metric_flags
    record["quality_flags"] = list(dict.fromkeys(flags))
    return record


def normalize_observation(payload: dict, provider_meta: ProviderMeta) -> dict:
    """Normalize a field observation payload into TerraSatch format."""
    raw_severity = (
        payload.get("severity")
        or payload.get("danger")
        or payload.get("hazard_level")
        or payload.get("risk")
    )
    raw_time = (
        payload.get("observed_at")
        or payload.get("timestamp")
        or payload.get("date")
        or payload.get("time")
    )
    summary = safe_str(
        payload.get("summary")
        or payload.get("notes")
        or payload.get("description")
        or payload.get("observation")
        or ""
    )
    region = safe_str(
        payload.get("region")
        or payload.get("zone")
        or payload.get("area")
        or ""
    ) or None

    raw_metrics = payload.get("metrics") or {}
    if not isinstance(raw_metrics, dict):
        raw_metrics = {}
    for key in ("snow_depth_cm", "new_snow_cm", "wind_speed_mph", "temperature_f", "aspect", "slope_angle"):
        val = payload.get(key)
        if val is not None:
            raw_metrics[key] = val
    metrics, metric_flags = validate_metrics(raw_metrics)

    record = {
        "provider_id": safe_str(provider_meta.get("provider_id")),
        "provider_name": safe_str(provider_meta.get("provider_name")),
        "record_type": "observation",
        "region": region,
        "location": _build_location(payload),
        "event_time": parse_timestamp(raw_time),
        "severity": normalize_severity(str(raw_severity) if raw_severity is not None else None),
        "metrics": metrics,
        "summary": summary,
    }
    flags = _collect_quality_flags(record) + metric_flags
    record["quality_flags"] = list(dict.fromkeys(flags))
    return record


def normalize_weather(payload: dict, provider_meta: ProviderMeta) -> dict:
    """Normalize a weather station snapshot payload into TerraSatch format."""
    raw_severity = (
        payload.get("severity")
        or payload.get("alert_level")
        or payload.get("warning_level")
    )
    raw_time = (
        payload.get("recorded_at")
        or payload.get("timestamp")
        or payload.get("observation_time")
        or payload.get("time")
        or payload.get("date")
    )
    summary = safe_str(
        payload.get("summary")
        or payload.get("conditions")
        or payload.get("description")
        or ""
    )
    region = safe_str(
        payload.get("region")
        or payload.get("zone")
        or payload.get("station_region")
        or ""
    ) or None

    raw_metrics = payload.get("metrics") or {}
    if not isinstance(raw_metrics, dict):
        raw_metrics = {}
    for key in (
        "temperature_f", "temperature_c", "wind_speed_mph", "wind_gust_mph",
        "wind_direction", "relative_humidity", "snow_depth_cm", "new_snow_cm",
        "pressure_mb", "visibility_miles",
    ):
        val = payload.get(key)
        if val is not None:
            raw_metrics[key] = val
    metrics, metric_flags = validate_metrics(raw_metrics)

    record = {
        "provider_id": safe_str(provider_meta.get("provider_id")),
        "provider_name": safe_str(provider_meta.get("provider_name")),
        "record_type": "weather",
        "region": region,
        "location": _build_location(payload),
        "event_time": parse_timestamp(raw_time),
        "severity": normalize_severity(str(raw_severity) if raw_severity is not None else None),
        "metrics": metrics,
        "summary": summary,
    }
    flags = _collect_quality_flags(record) + metric_flags
    record["quality_flags"] = list(dict.fromkeys(flags))
    return record


NORMALIZERS = {
    "bulletin": normalize_bulletin,
    "observation": normalize_observation,
    "weather": normalize_weather,
}


def dispatch_normalizer(record_type: str, payload: dict, provider_meta: ProviderMeta) -> dict:
    """Route payload to the correct normalizer by record_type."""
    normalizer = NORMALIZERS.get(record_type)
    if normalizer is None:
        raise ValueError(f"Unsupported record_type: '{record_type}'. Supported: {list(NORMALIZERS)}")
    return normalizer(payload, provider_meta)
