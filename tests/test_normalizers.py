"""Tests for normalizer functions."""
import pytest
from app.normalizers import (
    dispatch_normalizer,
    normalize_bulletin,
    normalize_observation,
    normalize_weather,
)


PROVIDER = {"provider_id": "test-provider", "provider_name": "Test Provider"}


class TestNormalizeBulletin:
    def test_basic_fields(self):
        payload = {
            "danger": "considerable",
            "issued_at": "2024-01-15T12:00:00Z",
            "summary": "High danger above treeline.",
            "region": "North Cascades",
            "lat": 48.5,
            "lon": -121.3,
        }
        result = normalize_bulletin(payload, PROVIDER)
        assert result["record_type"] == "bulletin"
        assert result["severity"] == "considerable"
        assert result["region"] == "North Cascades"
        assert result["location"]["lat"] == 48.5
        assert result["location"]["lon"] == -121.3
        assert "High danger" in result["summary"]
        assert "missing_coordinates" not in result["quality_flags"]
        assert "missing_timestamp" not in result["quality_flags"]

    def test_alternate_field_names(self):
        payload = {
            "avalanche_danger": "high",
            "timestamp": "2024-01-15T08:00:00Z",
            "headline": "Extreme conditions expected.",
            "latitude": 39.5,
            "longitude": -106.0,
        }
        result = normalize_bulletin(payload, PROVIDER)
        assert result["severity"] == "high"
        assert result["location"]["lat"] == 39.5
        assert result["location"]["lon"] == -106.0
        assert result["summary"] == "Extreme conditions expected."

    def test_numeric_severity(self):
        payload = {"danger": "4", "issued_at": "2024-01-01T00:00:00Z", "summary": "Test"}
        result = normalize_bulletin(payload, PROVIDER)
        assert result["severity"] == "high"

    def test_missing_coordinates_flag(self):
        payload = {"danger": "low", "issued_at": "2024-01-01T00:00:00Z", "summary": "Test"}
        result = normalize_bulletin(payload, PROVIDER)
        assert "missing_coordinates" in result["quality_flags"]

    def test_missing_timestamp_flag(self):
        payload = {"danger": "low", "summary": "Test", "lat": 40.0, "lon": -105.0}
        result = normalize_bulletin(payload, PROVIDER)
        assert "missing_timestamp" in result["quality_flags"]

    def test_missing_summary_flag(self):
        payload = {"danger": "low", "issued_at": "2024-01-01T00:00:00Z", "lat": 40.0, "lon": -105.0}
        result = normalize_bulletin(payload, PROVIDER)
        assert "empty_summary" in result["quality_flags"]

    def test_unknown_severity_flag(self):
        payload = {
            "danger": "banana",
            "issued_at": "2024-01-01T00:00:00Z",
            "summary": "Test",
            "lat": 40.0,
            "lon": -105.0,
        }
        result = normalize_bulletin(payload, PROVIDER)
        assert result["severity"] == "unknown"
        assert "unknown_severity" in result["quality_flags"]

    def test_nested_location(self):
        payload = {
            "danger": "moderate",
            "issued_at": "2024-01-01T00:00:00Z",
            "summary": "Moderate conditions.",
            "location": {"lat": 44.1, "lon": -110.5, "name": "Teton Pass", "elevation_ft": 8431},
        }
        result = normalize_bulletin(payload, PROVIDER)
        assert result["location"]["lat"] == 44.1
        assert result["location"]["lon"] == -110.5
        assert result["location"]["name"] == "Teton Pass"
        assert result["location"]["elevation_ft"] == 8431


class TestNormalizeObservation:
    def test_basic_fields(self):
        payload = {
            "observed_at": "2024-02-10T09:30:00Z",
            "notes": "Natural avalanche activity observed.",
            "region": "Sierra Nevada",
            "lat": 38.9,
            "lon": -120.0,
            "hazard_level": "considerable",
            "snow_depth_cm": 180,
        }
        result = normalize_observation(payload, PROVIDER)
        assert result["record_type"] == "observation"
        assert result["severity"] == "considerable"
        assert result["metrics"]["snow_depth_cm"] == 180
        assert "Natural avalanche" in result["summary"]

    def test_alternate_field_names(self):
        payload = {
            "timestamp": "2024-02-10T09:30:00Z",
            "observation": "Shooting cracks noted.",
            "latitude": 38.9,
            "longitude": -120.0,
            "risk": "high",
        }
        result = normalize_observation(payload, PROVIDER)
        assert result["severity"] == "high"
        assert result["summary"] == "Shooting cracks noted."

    def test_no_severity(self):
        payload = {
            "timestamp": "2024-02-10T09:30:00Z",
            "description": "Calm conditions.",
            "lat": 40.0,
            "lon": -105.0,
        }
        result = normalize_observation(payload, PROVIDER)
        assert result["severity"] == "unknown"
        assert "unknown_severity" in result["quality_flags"]


class TestNormalizeWeather:
    def test_basic_fields(self):
        payload = {
            "recorded_at": "2024-03-01T06:00:00Z",
            "conditions": "Clear and cold.",
            "lat": 46.8,
            "lon": -121.7,
            "temperature_f": 18.0,
            "wind_speed_mph": 25,
            "new_snow_cm": 5,
        }
        result = normalize_weather(payload, PROVIDER)
        assert result["record_type"] == "weather"
        assert result["metrics"]["temperature_f"] == 18.0
        assert result["metrics"]["wind_speed_mph"] == 25
        assert result["metrics"]["new_snow_cm"] == 5
        assert result["summary"] == "Clear and cold."

    def test_alternate_timestamp_fields(self):
        payload = {
            "observation_time": "2024-03-01T06:00:00Z",
            "description": "Blowing snow.",
            "lat": 46.8,
            "lon": -121.7,
        }
        result = normalize_weather(payload, PROVIDER)
        assert result["event_time"] is not None

    def test_invalid_metric_type_flag(self):
        payload = {
            "timestamp": "2024-03-01T06:00:00Z",
            "description": "Icy.",
            "lat": 46.8,
            "lon": -121.7,
            "metrics": {"complex_val": [1, 2, 3]},
        }
        result = normalize_weather(payload, PROVIDER)
        assert "invalid_metric_type" in result["quality_flags"]


class TestDispatcher:
    def test_routes_bulletin(self):
        payload = {"danger": "low", "issued_at": "2024-01-01T00:00:00Z", "summary": "Test"}
        result = dispatch_normalizer("bulletin", payload, PROVIDER)
        assert result["record_type"] == "bulletin"

    def test_routes_observation(self):
        payload = {"timestamp": "2024-01-01T00:00:00Z", "notes": "Test"}
        result = dispatch_normalizer("observation", payload, PROVIDER)
        assert result["record_type"] == "observation"

    def test_routes_weather(self):
        payload = {"timestamp": "2024-01-01T00:00:00Z", "conditions": "Clear"}
        result = dispatch_normalizer("weather", payload, PROVIDER)
        assert result["record_type"] == "weather"

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported record_type"):
            dispatch_normalizer("forecast", {}, PROVIDER)
