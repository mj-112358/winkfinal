"""
SQLAlchemy models for the Wink AI retail analytics platform.
All models include store_id for multi-tenant isolation.
"""

from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, Float, ForeignKey, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB
from datetime import datetime
import uuid

Base = declarative_base()

class Store(Base):
    __tablename__ = "stores"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    timezone = Column(String(50), default="UTC")
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    users = relationship("User", back_populates="store")
    cameras = relationship("Camera", back_populates="store")

class User(Base):
    __tablename__ = "users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(50), default="manager")  # store_owner, manager, viewer
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at = Column(DateTime)
    
    # Relationships
    store = relationship("Store", back_populates="users")
    
    __table_args__ = (
        Index("idx_users_store_email", "store_id", "email"),
    )

class Camera(Base):
    __tablename__ = "cameras"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    name = Column(String(255), nullable=False)
    rtsp_url = Column(String(512), nullable=False)
    section = Column(String(255))  # New field for camera section/area
    status = Column(String(50), default="offline")  # live, connecting, offline, error
    last_heartbeat_at = Column(DateTime)
    last_error = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    store = relationship("Store", back_populates="cameras")
    zones = relationship("Zone", back_populates="camera")
    tracks = relationship("Track", back_populates="camera")
    events = relationship("Event", back_populates="camera")
    
    __table_args__ = (
        Index("idx_cameras_store", "store_id"),
        Index("idx_cameras_status", "status"),
    )

class Zone(Base):
    __tablename__ = "zones"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    camera_id = Column(UUID(as_uuid=True), ForeignKey("cameras.id"), nullable=False)
    name = Column(String(255), nullable=False)
    ztype = Column(String(50), nullable=False)  # entrance, checkout, product_area, queue
    polygon_json = Column(JSONB, nullable=False)
    screenshot_path = Column(String(512))
    screenshot_w = Column(Integer)
    screenshot_h = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    camera = relationship("Camera", back_populates="zones")
    events = relationship("Event", back_populates="zone")
    
    __table_args__ = (
        Index("idx_zones_store_camera", "store_id", "camera_id"),
    )

class Track(Base):
    __tablename__ = "tracks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    camera_id = Column(UUID(as_uuid=True), ForeignKey("cameras.id"), nullable=False)
    person_uuid = Column(String(255), nullable=False)  # Re-ID tracking UUID
    first_seen_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False)
    
    # Relationships
    camera = relationship("Camera", back_populates="tracks")
    events = relationship("Event", back_populates="track")
    
    __table_args__ = (
        Index("idx_tracks_store_person", "store_id", "person_uuid"),
        Index("idx_tracks_camera_person_date", "camera_id", "person_uuid", "first_seen_at"),
        UniqueConstraint("store_id", "camera_id", "person_uuid", name="unique_track_per_camera_person"),
    )

class Event(Base):
    __tablename__ = "events"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    camera_id = Column(UUID(as_uuid=True), ForeignKey("cameras.id"), nullable=False)
    zone_id = Column(UUID(as_uuid=True), ForeignKey("zones.id"), nullable=True)
    track_id = Column(UUID(as_uuid=True), ForeignKey("tracks.id"), nullable=True)
    person_uuid = Column(String(255), nullable=False)
    event_type = Column(String(50), nullable=False)  # entry, exit, queue_join, queue_leave, product_interaction
    ts = Column(DateTime, nullable=False)
    payload_json = Column(JSONB)
    
    # Relationships
    camera = relationship("Camera", back_populates="events")
    zone = relationship("Zone", back_populates="events")
    track = relationship("Track", back_populates="events")
    
    __table_args__ = (
        Index("idx_events_store_ts", "store_id", "ts"),
        Index("idx_events_camera_ts", "camera_id", "ts"),
        Index("idx_events_person", "person_uuid"),
        Index("idx_events_type_ts", "event_type", "ts"),
    )

class MetricsDaily(Base):
    __tablename__ = "metrics_daily"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    date = Column(String(10), nullable=False)  # YYYY-MM-DD
    camera_id = Column(UUID(as_uuid=True), ForeignKey("cameras.id"), nullable=True)
    zone_id = Column(UUID(as_uuid=True), ForeignKey("zones.id"), nullable=True)
    metric = Column(String(50), nullable=False)  # footfall, unique_visitors, dwell_avg, queue_wait_avg, etc.
    value = Column(Float, nullable=False)
    meta_json = Column(JSONB)
    
    __table_args__ = (
        Index("idx_metrics_store_date", "store_id", "date"),
        Index("idx_metrics_store_date_camera", "store_id", "date", "camera_id"),
        Index("idx_metrics_store_date_zone", "store_id", "date", "zone_id"),
        UniqueConstraint("store_id", "date", "camera_id", "zone_id", "metric", name="unique_daily_metric"),
    )

class Insight(Base):
    __tablename__ = "insights"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    period_start = Column(String(10), nullable=False)  # YYYY-MM-DD
    period_end = Column(String(10), nullable=False)    # YYYY-MM-DD
    payload_json = Column(JSONB, nullable=False)
    promo_context_json = Column(JSONB)
    festival_context_json = Column(JSONB)
    ai_insights = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_insights_store_period", "store_id", "period_start", "period_end"),
    )

class PromotionEvent(Base):
    __tablename__ = "promotion_events"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    name = Column(String(255), nullable=False)
    event_type = Column(String(50), nullable=False)  # promotion, festival, sale
    start_date = Column(String(10), nullable=False)
    end_date = Column(String(10), nullable=False)
    description = Column(Text)
    metadata_json = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_promo_events_store_dates", "store_id", "start_date", "end_date"),
    )

class Invite(Base):
    __tablename__ = "invites"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID(as_uuid=True), ForeignKey("stores.id"), nullable=False)
    email = Column(String(255), nullable=False)
    role = Column(String(50), default="manager")
    invite_token = Column(String(255), unique=True, nullable=False)
    invited_by = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    expires_at = Column(DateTime, nullable=False)
    accepted_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_invites_token", "invite_token"),
        Index("idx_invites_email", "email"),
    )