import json
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy.sql import func
from app.database import Base


class NormalizedRecord(Base):
    __tablename__ = "normalized_records"

    record_id = Column(String, primary_key=True, index=True)
    provider_id = Column(String, index=True, nullable=False)
    provider_name = Column(String, nullable=False)
    record_type = Column(String, index=True, nullable=False)
    region = Column(String, nullable=True)
    location_json = Column(Text, nullable=False, default="{}")
    event_time = Column(String, nullable=True)
    ingested_at = Column(String, nullable=False)
    severity = Column(String, index=True, nullable=False, default="unknown")
    metrics_json = Column(Text, nullable=False, default="{}")
    summary = Column(Text, nullable=False, default="")
    raw_payload_json = Column(Text, nullable=False, default="{}")
    quality_flags_json = Column(Text, nullable=False, default="[]")
    created_at = Column(DateTime, server_default=func.now())

    @property
    def location(self):
        return json.loads(self.location_json)

    @property
    def metrics(self):
        return json.loads(self.metrics_json)

    @property
    def raw_payload(self):
        return json.loads(self.raw_payload_json)

    @property
    def quality_flags(self):
        return json.loads(self.quality_flags_json)
