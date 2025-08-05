from uuid import uuid1
#from sqlalchemy import UUID, Column, DateTime, Integer, String, Float, ForeignKey, func
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Date, Time, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from geoalchemy2 import Geometry
from app.database import Base
from sqlalchemy.orm import relationship

class Source(Base):
    __tablename__ = "source"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)

class SubSource(Base):
    __tablename__ = "sub_source"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    source_id = Column(Integer, ForeignKey("source.id"))

class MaritimeDataCDF(Base):
    __tablename__ = "cdf_data"
    id = Column(Integer, primary_key=True, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    speed = Column(Float)
    bearing = Column(Float)
    course = Column(Float)
    altitude = Column(Float, nullable=True)
    sys_trk_no = Column(Integer, nullable=True)
    source_id = Column(Integer, ForeignKey("source.id"))
    sub_source_id = Column(Integer, ForeignKey("sub_source.id"))
    location = Column(Geometry("POINT"), nullable=True)
    raw_data = Column(JSONB)
    uuid = Column(UUID(as_uuid=True), unique=True, nullable=False)
    logged_timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    file_uuid = Column(UUID(as_uuid=True), unique=True, nullable=False)
    
    # Newly added fields
    msg_date = Column(Date, nullable=True)
    msg_time = Column(Time, nullable=True)
    change_type = Column(String, nullable=True)
    trk_short_name = Column(String, nullable=True)
    position_valid_date = Column(Date, nullable=True)
    position_valid_time = Column(Time, nullable=True)
    trk_display_number = Column(String, nullable=True)
    sub_source_name = Column(String, nullable=True)




class UploadMetadata(Base):
    __tablename__ = "upload_metadata"

    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String, nullable=True)
    source_id = Column(Integer, ForeignKey("source.id"))
    sub_source_id = Column(Integer, ForeignKey("sub_source.id"))
    format = Column(String, nullable=True)  # e.g., 'NDJSON', 'CSV', 'Excel'
    record_count = Column(Integer)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

    source = relationship("Source")
    sub_source = relationship("SubSource")
