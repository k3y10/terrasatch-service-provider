import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import NormalizedRecord
from app.normalizers import dispatch_normalizer
from app.schemas import (
    BatchIngestRequest,
    BatchIngestResponse,
    BatchRecordResult,
    HealthResponse,
    IngestRequest,
    IngestResponse,
    NormalizedRecordSchema,
    LocationSchema,
)
from app.utils import now_iso

router = APIRouter()


def _record_to_schema(record: NormalizedRecord) -> NormalizedRecordSchema:
    loc = record.location
    return NormalizedRecordSchema(
        record_id=record.record_id,
        provider_id=record.provider_id,
        provider_name=record.provider_name,
        record_type=record.record_type,
        region=record.region,
        location=LocationSchema(**loc),
        event_time=record.event_time,
        ingested_at=record.ingested_at,
        severity=record.severity,
        metrics=record.metrics,
        summary=record.summary,
        raw_payload=record.raw_payload,
        quality_flags=record.quality_flags,
    )


def _process_ingest(req: IngestRequest, db: Session) -> NormalizedRecord:
    provider_meta = {
        "provider_id": req.provider_id,
        "provider_name": req.provider_name,
    }
    normalized = dispatch_normalizer(req.record_type, req.payload, provider_meta)

    record_id = str(uuid.uuid4())
    ingested_at = now_iso()

    db_record = NormalizedRecord(
        record_id=record_id,
        provider_id=normalized["provider_id"],
        provider_name=normalized["provider_name"],
        record_type=normalized["record_type"],
        region=normalized.get("region"),
        location_json=json.dumps(normalized["location"]),
        event_time=normalized.get("event_time"),
        ingested_at=ingested_at,
        severity=normalized["severity"],
        metrics_json=json.dumps(normalized["metrics"]),
        summary=normalized["summary"],
        raw_payload_json=json.dumps(req.payload),
        quality_flags_json=json.dumps(normalized["quality_flags"]),
    )
    db.add(db_record)
    db.commit()
    db.refresh(db_record)
    return db_record


@router.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok", version="1.0.0")


@router.post("/ingest", response_model=IngestResponse)
def ingest(req: IngestRequest, db: Session = Depends(get_db)):
    try:
        db_record = _process_ingest(req, db)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    schema = _record_to_schema(db_record)
    return IngestResponse(record=schema, quality_flags=schema.quality_flags)


@router.post("/ingest/batch", response_model=BatchIngestResponse)
def ingest_batch(req: BatchIngestRequest, db: Session = Depends(get_db)):
    results = []
    success_count = 0
    for item in req.records:
        try:
            db_record = _process_ingest(item, db)
            schema = _record_to_schema(db_record)
            results.append(BatchRecordResult(success=True, record=schema))
            success_count += 1
        except Exception as exc:
            results.append(BatchRecordResult(success=False, error=str(exc)))
    return BatchIngestResponse(
        total_count=len(req.records),
        success_count=success_count,
        failed_count=len(req.records) - success_count,
        results=results,
    )


@router.get("/records", response_model=list[NormalizedRecordSchema])
def list_records(
    provider_id: str = Query(default=None),
    record_type: str = Query(default=None),
    severity: str = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    query = db.query(NormalizedRecord)
    if provider_id:
        query = query.filter(NormalizedRecord.provider_id == provider_id)
    if record_type:
        query = query.filter(NormalizedRecord.record_type == record_type)
    if severity:
        query = query.filter(NormalizedRecord.severity == severity)
    records = query.order_by(NormalizedRecord.ingested_at.desc()).limit(limit).all()
    return [_record_to_schema(r) for r in records]


@router.get("/records/context")
def records_context(
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    records = (
        db.query(NormalizedRecord)
        .order_by(NormalizedRecord.ingested_at.desc())
        .limit(limit)
        .all()
    )
    lines = []
    for r in records:
        loc = r.location
        lat = loc.get("lat")
        lon = loc.get("lon")
        coord_str = f"{lat},{lon}" if lat is not None and lon is not None else "unknown"
        line = (
            f"[{r.record_type}] {r.provider_name} | {r.region or 'unknown region'} | "
            f"severity={r.severity} | coords={coord_str} | "
            f"event_time={r.event_time or 'unknown'} | {r.summary[:120] if r.summary else '(no summary)'}"
        )
        lines.append(line)
    return {"count": len(lines), "context": lines}


@router.get("/records/{record_id}", response_model=NormalizedRecordSchema)
def get_record(record_id: str, db: Session = Depends(get_db)):
    record = db.query(NormalizedRecord).filter(NormalizedRecord.record_id == record_id).first()
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return _record_to_schema(record)
