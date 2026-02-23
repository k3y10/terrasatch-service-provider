from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class LocationSchema(BaseModel):
    name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    elevation_ft: Optional[int] = None


class NormalizedRecordSchema(BaseModel):
    record_id: str
    provider_id: str
    provider_name: str
    record_type: str
    region: Optional[str] = None
    location: LocationSchema = Field(default_factory=LocationSchema)
    event_time: Optional[str] = None
    ingested_at: str
    severity: str = "unknown"
    metrics: Dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    raw_payload: Dict[str, Any] = Field(default_factory=dict)
    quality_flags: List[str] = Field(default_factory=list)


class IngestRequest(BaseModel):
    provider_id: str
    provider_name: str
    record_type: str
    payload: Dict[str, Any]


class IngestResponse(BaseModel):
    record: NormalizedRecordSchema
    quality_flags: List[str]


class BatchIngestRequest(BaseModel):
    records: List[IngestRequest]


class BatchRecordResult(BaseModel):
    success: bool
    record: Optional[NormalizedRecordSchema] = None
    error: Optional[str] = None


class BatchIngestResponse(BaseModel):
    total_count: int
    success_count: int
    failed_count: int
    results: List[BatchRecordResult]


class HealthResponse(BaseModel):
    status: str
    version: str
