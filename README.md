# TerraSatch Service Provider

Central ingestion service for TerraSatch provider data.

## What This Server Does

This is the **main TerraSatch ingestion server**. It receives JSON payloads from different weather and avalanche data providers, validates and cleans the data, normalizes it into a single consistent TerraSatch schema, stores both the normalized record and the original raw payload in SQLite, and returns structured outputs with quality flags.

Downstream systems (SherpAI, dashboards, analytics, chat/RAG) consume the normalized records from this service.

**Supported record types (MVP):**
- `bulletin` — forecast bulletins (avalanche danger ratings, conditions summaries)
- `observation` — field observations (snowpack, avalanche activity)
- `weather` — weather station snapshots (temperature, wind, snow depth)

---

## Tech Stack

- Python 3.11+
- FastAPI
- Pydantic v2
- SQLite (local, file-based)
- SQLAlchemy 2.x
- Uvicorn
- pytest

---

## How to Run Locally

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

Interactive API docs (Swagger UI): `http://localhost:8000/docs`

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Server health check |
| POST | `/ingest` | Ingest one record |
| POST | `/ingest/batch` | Ingest a batch of records |
| GET | `/records` | List normalized records (with optional filters) |
| GET | `/records/context` | Compact text-friendly context for SherpAI |
| GET | `/records/{record_id}` | Retrieve one record by ID |

**Query parameters for `GET /records`:**
- `provider_id` — filter by provider
- `record_type` — filter by type (`bulletin`, `observation`, `weather`)
- `severity` — filter by severity level
- `limit` — max results (default 50, max 500)

---

## Example curl Requests

### GET /health

```bash
curl http://localhost:8000/health
```

Response:
```json
{"status": "ok", "version": "1.0.0"}
```

---

### POST /ingest

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "provider_id": "nwac-001",
    "provider_name": "Northwest Avalanche Center",
    "record_type": "bulletin",
    "payload": {
      "danger": "considerable",
      "issued_at": "2024-01-15T12:00:00Z",
      "summary": "Considerable danger on steep slopes above 5000 ft.",
      "region": "North Cascades",
      "lat": 48.5,
      "lon": -121.3
    }
  }'
```

---

### POST /ingest/batch

```bash
curl -X POST http://localhost:8000/ingest/batch \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "provider_id": "nwac-001",
        "provider_name": "Northwest Avalanche Center",
        "record_type": "bulletin",
        "payload": {
          "danger": "high",
          "issued_at": "2024-01-16T08:00:00Z",
          "summary": "High danger. Avoid avalanche terrain.",
          "lat": 48.5,
          "lon": -121.3
        }
      },
      {
        "provider_id": "snotel-012",
        "provider_name": "SNOTEL Station 012",
        "record_type": "weather",
        "payload": {
          "timestamp": "2024-01-16T06:00:00Z",
          "conditions": "Clear and cold overnight.",
          "latitude": 47.8,
          "longitude": -120.9,
          "temperature_f": 14.0,
          "wind_speed_mph": 12,
          "new_snow_cm": 3
        }
      }
    ]
  }'
```

---

## Example Provider Payloads (Different Field Names)

The server handles field-name differences automatically:

### Provider A — Standard format
```json
{
  "provider_id": "prov-a",
  "provider_name": "Provider A",
  "record_type": "bulletin",
  "payload": {
    "danger": "considerable",
    "issued_at": "2024-01-15T12:00:00Z",
    "summary": "Considerable danger.",
    "lat": 44.1,
    "lon": -110.5
  }
}
```

### Provider B — Alternate field names
```json
{
  "provider_id": "prov-b",
  "provider_name": "Provider B",
  "record_type": "bulletin",
  "payload": {
    "avalanche_danger": "3",
    "timestamp": "2024-01-15T12:00:00Z",
    "headline": "Considerable danger on upper mountain.",
    "latitude": 44.1,
    "longitude": -110.5
  }
}
```

### Provider C — Nested location
```json
{
  "provider_id": "prov-c",
  "provider_name": "Provider C",
  "record_type": "observation",
  "payload": {
    "observed_at": "2024-02-10T09:00:00Z",
    "notes": "Natural avalanche observed on north aspect.",
    "hazard_level": "high",
    "location": {
      "name": "Teton Pass",
      "lat": 43.5,
      "lon": -110.9,
      "elevation_ft": 8431
    },
    "snow_depth_cm": 180
  }
}
```

Both Provider A and Provider B produce the **same normalized TerraSatch output**.

---

## Example Normalized TerraSatch Output

```json
{
  "record_id": "3f2a1b4c-8e9d-4f0a-b1c2-d3e4f5a6b7c8",
  "provider_id": "prov-b",
  "provider_name": "Provider B",
  "record_type": "bulletin",
  "region": null,
  "location": {
    "name": null,
    "lat": 44.1,
    "lon": -110.5,
    "elevation_ft": null
  },
  "event_time": "2024-01-15T12:00:00+00:00",
  "ingested_at": "2024-01-15T13:05:22.441238+00:00",
  "severity": "considerable",
  "metrics": {},
  "summary": "Considerable danger on upper mountain.",
  "raw_payload": {
    "avalanche_danger": "3",
    "timestamp": "2024-01-15T12:00:00Z",
    "headline": "Considerable danger on upper mountain.",
    "latitude": 44.1,
    "longitude": -110.5
  },
  "quality_flags": []
}
```

**Quality flags explained:**
- `missing_coordinates` — `lat` or `lon` is null
- `missing_timestamp` — `event_time` could not be parsed
- `unknown_severity` — severity could not be mapped to a known level
- `empty_summary` — summary field is blank
- `invalid_metric_type` — a metric value is a non-scalar type

---

## Project Structure

```
terrasatch-service-provider/
├── app/
│   ├── __init__.py
│   ├── main.py          # FastAPI app entry point + lifespan
│   ├── routes.py        # All API endpoints
│   ├── schemas.py       # Pydantic request/response models
│   ├── models.py        # SQLAlchemy ORM model
│   ├── database.py      # SQLite engine + session + init
│   ├── normalizers.py   # Core normalization logic
│   └── utils.py         # Helpers (timestamp parsing, severity mapping)
├── tests/
│   ├── __init__.py
│   ├── test_ingest.py       # API endpoint integration tests
│   └── test_normalizers.py  # Unit tests for normalization logic
├── requirements.txt
└── README.md
```

---

## Adding a New Provider Mapping

To support a new provider with different field names:

1. Open `app/normalizers.py`.
2. Find the normalizer function for the relevant record type (e.g. `normalize_bulletin`).
3. Add the new field name to the fallback chain using `or`:

```python
# Before
raw_severity = (
    payload.get("danger")
    or payload.get("avalanche_danger")
    or payload.get("severity")
)

# After — add your new provider's field name
raw_severity = (
    payload.get("danger")
    or payload.get("avalanche_danger")
    or payload.get("severity")
    or payload.get("new_provider_danger_field")  # <-- add here
)
```

4. Run `pytest tests/ -v` to confirm nothing is broken.

To add an entirely new record type (e.g. `snowpack`):

1. Add `normalize_snowpack(payload, provider_meta)` to `app/normalizers.py`.
2. Register it in the `NORMALIZERS` dict at the bottom of `normalizers.py`.
3. Add tests in `tests/test_normalizers.py`.
