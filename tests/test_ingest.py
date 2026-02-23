"""Integration tests for API endpoints."""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.main import app

TEST_DATABASE_URL = "sqlite:///./test_terrasatch.db"
engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    app.dependency_overrides[get_db] = override_get_db
    yield
    Base.metadata.drop_all(bind=engine)
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app)


class TestHealth:
    def test_health_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestIngest:
    def test_ingest_bulletin(self, client):
        payload = {
            "provider_id": "prov-001",
            "provider_name": "Mountain Weather Co",
            "record_type": "bulletin",
            "payload": {
                "danger": "considerable",
                "issued_at": "2024-01-15T12:00:00Z",
                "summary": "High danger on steep slopes.",
                "region": "North Cascades",
                "lat": 48.5,
                "lon": -121.3,
            },
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["record"]["record_type"] == "bulletin"
        assert data["record"]["severity"] == "considerable"
        assert data["record"]["provider_id"] == "prov-001"
        assert "record_id" in data["record"]
        assert "ingested_at" in data["record"]
        assert data["record"]["raw_payload"]["danger"] == "considerable"

    def test_ingest_observation(self, client):
        payload = {
            "provider_id": "prov-002",
            "provider_name": "Field Observer",
            "record_type": "observation",
            "payload": {
                "observed_at": "2024-02-10T09:00:00Z",
                "notes": "Natural avalanche activity.",
                "latitude": 39.5,
                "longitude": -106.0,
                "hazard_level": "high",
                "snow_depth_cm": 200,
            },
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["record"]["record_type"] == "observation"
        assert data["record"]["severity"] == "high"
        assert data["record"]["location"]["lat"] == 39.5
        assert data["record"]["metrics"]["snow_depth_cm"] == 200

    def test_ingest_weather(self, client):
        payload = {
            "provider_id": "prov-003",
            "provider_name": "Weather Station Network",
            "record_type": "weather",
            "payload": {
                "recorded_at": "2024-03-01T06:00:00Z",
                "conditions": "Clear and cold.",
                "lat": 46.8,
                "lon": -121.7,
                "temperature_f": 18.0,
                "wind_speed_mph": 25,
            },
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["record"]["record_type"] == "weather"
        assert data["record"]["metrics"]["temperature_f"] == 18.0

    def test_ingest_unsupported_type_returns_422(self, client):
        payload = {
            "provider_id": "prov-001",
            "provider_name": "Test",
            "record_type": "forecast",
            "payload": {},
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 422

    def test_ingest_quality_flags_returned(self, client):
        payload = {
            "provider_id": "prov-001",
            "provider_name": "Test",
            "record_type": "bulletin",
            "payload": {},
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 200
        flags = response.json()["quality_flags"]
        assert "missing_coordinates" in flags
        assert "missing_timestamp" in flags
        assert "empty_summary" in flags

    def test_ingest_alternate_field_names(self, client):
        payload = {
            "provider_id": "prov-alt",
            "provider_name": "Alt Provider",
            "record_type": "bulletin",
            "payload": {
                "source_name": "Alt Provider",
                "avalanche_danger": "3",
                "timestamp": "2024-01-20T10:00:00Z",
                "headline": "Considerable danger on upper mountain.",
                "latitude": 44.1,
                "longitude": -110.5,
            },
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["record"]["severity"] == "considerable"
        assert data["record"]["location"]["lat"] == 44.1


class TestBatchIngest:
    def test_batch_ingest(self, client):
        payload = {
            "records": [
                {
                    "provider_id": "prov-001",
                    "provider_name": "Provider A",
                    "record_type": "bulletin",
                    "payload": {
                        "danger": "high",
                        "issued_at": "2024-01-15T12:00:00Z",
                        "summary": "High danger.",
                        "lat": 48.5,
                        "lon": -121.3,
                    },
                },
                {
                    "provider_id": "prov-002",
                    "provider_name": "Provider B",
                    "record_type": "observation",
                    "payload": {
                        "timestamp": "2024-01-16T08:00:00Z",
                        "notes": "Wet avalanche cycle.",
                        "lat": 39.5,
                        "lon": -106.0,
                    },
                },
            ]
        }
        response = client.post("/ingest/batch", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 2
        assert data["success_count"] == 2
        assert data["failed_count"] == 0
        assert len(data["results"]) == 2
        assert all(r["success"] for r in data["results"])

    def test_batch_ingest_partial_failure(self, client):
        payload = {
            "records": [
                {
                    "provider_id": "prov-001",
                    "provider_name": "Provider A",
                    "record_type": "bulletin",
                    "payload": {"danger": "high", "issued_at": "2024-01-15T12:00:00Z", "summary": "Test"},
                },
                {
                    "provider_id": "prov-bad",
                    "provider_name": "Bad Provider",
                    "record_type": "invalid_type",
                    "payload": {},
                },
            ]
        }
        response = client.post("/ingest/batch", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 2
        assert data["success_count"] == 1
        assert data["failed_count"] == 1


class TestRecords:
    PAYLOADS = {
        "bulletin": {
            "danger": "moderate",
            "issued_at": "2024-01-15T12:00:00Z",
            "summary": "Moderate conditions.",
            "lat": 48.5,
            "lon": -121.3,
        },
        "observation": {
            "observed_at": "2024-01-15T12:00:00Z",
            "notes": "Moderate snowpack.",
            "lat": 48.5,
            "lon": -121.3,
            "hazard_level": "moderate",
        },
        "weather": {
            "recorded_at": "2024-01-15T12:00:00Z",
            "conditions": "Clear and cold.",
            "lat": 48.5,
            "lon": -121.3,
            "temperature_f": 20.0,
        },
    }

    def _ingest_one(self, client, record_type="bulletin"):
        payload = {
            "provider_id": "prov-001",
            "provider_name": "Test Provider",
            "record_type": record_type,
            "payload": self.PAYLOADS[record_type],
        }
        return client.post("/ingest", json=payload).json()

    def test_list_records(self, client):
        self._ingest_one(client)
        response = client.get("/records")
        assert response.status_code == 200
        assert isinstance(response.json(), list)
        assert len(response.json()) >= 1

    def test_filter_by_provider_id(self, client):
        self._ingest_one(client)
        response = client.get("/records?provider_id=prov-001")
        assert response.status_code == 200
        assert all(r["provider_id"] == "prov-001" for r in response.json())

    def test_filter_by_record_type(self, client):
        self._ingest_one(client, "bulletin")
        self._ingest_one(client, "weather")
        response = client.get("/records?record_type=bulletin")
        assert response.status_code == 200
        assert all(r["record_type"] == "bulletin" for r in response.json())

    def test_filter_by_severity(self, client):
        self._ingest_one(client)
        response = client.get("/records?severity=moderate")
        assert response.status_code == 200
        assert all(r["severity"] == "moderate" for r in response.json())

    def test_get_record_by_id(self, client):
        ingested = self._ingest_one(client)
        record_id = ingested["record"]["record_id"]
        response = client.get(f"/records/{record_id}")
        assert response.status_code == 200
        assert response.json()["record_id"] == record_id

    def test_get_record_not_found(self, client):
        response = client.get("/records/nonexistent-id")
        assert response.status_code == 404

    def test_records_context(self, client):
        self._ingest_one(client)
        response = client.get("/records/context")
        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        assert "context" in data
        assert data["count"] >= 1
        assert isinstance(data["context"], list)
