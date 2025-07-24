import uuid
from fastapi import APIRouter, UploadFile, File, Form, Depends, Query, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
import json
import logging
from app.database import SessionLocal
from app.models import MaritimeDataCDF, Source, SubSource, UploadMetadata
from app.etl import convert_to_ais, convert_to_cdf, convert_to_cdf_from_csv_row, convert_to_piracy, parse_structured_file
from geoalchemy2.shape import to_shape
import pandas as pd
from fastapi.responses import StreamingResponse
from io import BytesIO
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
import uuid
from datetime import datetime, timezone
import io
import os                  
from app.config import MINIO_CLIENT, METADATA_COLLECTION


router = APIRouter()

# ------------------ DB SESSION ------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ------------------ SPARK ETL ------------------
def run_spark_etl(data: list[dict]):
    spark = SparkSession.builder \
        .appName("MaritimeETL") \
        .master("local[*]") \
        .getOrCreate()

    df = pd.DataFrame(data)
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("cdf_data_view")

    result = spark.sql("SELECT latitude, longitude, speed FROM cdf_data_view WHERE speed > 10")
    result.toPandas().to_csv("refined_output.csv", index=False)

    return result

# ------------------ FILE DETECTION ENTRY ------------------
@router.post("/upload/detect")
async def upload_detected_file(
    source: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        content_type = file.content_type
        filename = file.filename.lower()
        print(f"Detected file type: {content_type}, filename: {filename}")

        if content_type in ["application/x-ndjson", "application/json"] or filename.endswith(".ndjson"):
            return await upload_file(upload_source=source, file=file, db=db)

        elif content_type in ["text/csv", "application/vnd.ms-excel"] or filename.endswith(".csv"):
            return await upload_structured(upload_source=source, file=file, db=db)

        elif content_type.startswith("image/") or filename.endswith((".jpg", ".jpeg", ".png")):
            return {"status": "Image file processed", "filename": file.filename}

        elif content_type.startswith("audio/") or filename.endswith((".mp3", ".wav")):
            return {"status": "Audio file processed", "filename": file.filename}

        elif content_type == "application/pdf" or filename.endswith(".pdf"):
            return {"status": "PDF file processed", "filename": file.filename}

        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {content_type}")

    except Exception as e:
        logging.exception("Detection upload failed")
        raise HTTPException(status_code=500,  detail=str(e))


# ------------------ NDJSON UPLOAD ------------------
async def upload_file(
    upload_source: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        content = await file.read()
        lines = content.decode("utf-8").splitlines()

        source = db.query(Source).filter(func.lower(Source.name) == upload_source.lower()).first()
        sub_source = db.query(SubSource).first()

        file_uuid = uuid.uuid4()  # File-level UUID
        inserted = 0
        for line in lines:
            if not line.strip():
                continue
            try:
                record_uuid = uuid.uuid4()  # Per-record UUID
                json_obj = json.loads(line)

                # Choose the correct ETL converter
                if upload_source.lower() == "piracy":
                    cdf_data = convert_to_piracy(json_obj, record_uuid)
                elif upload_source.lower() == "ais":
                    cdf_data = convert_to_ais(json_obj, record_uuid)
                elif upload_source.lower() == "dmas":
                    cdf_data = convert_to_cdf(json_obj, record_uuid,file_uuid)
                elif upload_source.lower() == "p8i":
                    cdf_data = convert_to_cdf_from_csv_row(json_obj, record_uuid)
                else:
                    continue

                db_data = MaritimeDataCDF(
                    **cdf_data,
                    uuid=record_uuid,
                    file_uuid=file_uuid,
                    source_id=source.id if source else 1,
                    sub_source_id=sub_source.id if sub_source else 1
                )
                db.add(db_data)
                db.commit()
                inserted += 1
            except Exception as e:
                logging.warning(f"Skipping line due to error: {e}")

        minio_result = await upload_file_to_minio(file)
        if "error" in minio_result:
            raise HTTPException(status_code=400, detail=f"MinIO upload failed: {minio_result['error']}")

        mongo_result = store_metadata_in_mongodb(minio_result)
        if "error" in mongo_result:
            raise HTTPException(status_code=400, detail=f"MongoDB insert failed: {mongo_result['error']}")

        return {
            "message": f"{inserted} records saved",
            "file_uuid": file_uuid,
            "minio_metadata": minio_result,
            "type_of_data": upload_source
        }

    except Exception as e:
        logging.exception("Failed to process file")
        raise HTTPException(status_code=500, detail="Failed to process file")

async def upload_structured(
    upload_source: str = Query(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        file_uuid = str(uuid.uuid4())  # File-level UUID

        # Read content first to reuse it in parse + minio upload
        file_content = await file.read()
        file.file.seek(0)  # Reset file pointer for reuse later

        df = parse_structured_file(file_content)
        df.columns = df.columns.str.strip().str.replace("\u00a0", " ").str.replace("\t", " ").str.replace(r"\s+", " ", regex=True)

        source = db.query(Source).filter(func.lower(Source.name) == upload_source.lower()).first()
        sub_source = db.query(SubSource).first()

        inserted = 0
        failed_rows = []

        for i, row in df.iterrows():
            record_uuid = str(uuid.uuid4())  # Per-record UUID
            try:
                if upload_source.lower() == "piracy":
                    cdf_data = convert_to_piracy(row, record_uuid)
                elif upload_source.lower() == "ais":
                    cdf_data = convert_to_ais(row, record_uuid)
                elif upload_source.lower() == "dmas":
                    cdf_data = convert_to_cdf(row, record_uuid)
                elif upload_source.lower() == "p8i":
                    cdf_data = convert_to_cdf_from_csv_row(row, record_uuid)
                else:
                    failed_rows.append(f"Row {i} skipped: unknown source type")
                    continue

                db_data = MaritimeDataCDF(
                    **cdf_data,
                    uuid=record_uuid,
                    file_uuid=file_uuid,
                    source_id=source.id if source else 1,
                    sub_source_id=sub_source.id if sub_source else 1
                )
                db.add(db_data)
                inserted += 1
            except Exception as e:
                failed_rows.append(f"Row {i} failed: {e}")
                logging.warning(f"Row {i} failed: {e}")

        db.commit()

        # Upload only after successful parsing
        file_stream = io.BytesIO(file_content)
        file_stream.seek(0)
        file.filename = file.filename  # required by UploadFile wrapper

        minio_result = await upload_file_to_minio(file)
        if "error" in minio_result:
            raise HTTPException(status_code=400, detail=f"MinIO upload failed: {minio_result['error']}")

        mongo_result = store_metadata_in_mongodb(minio_result)
        if "error" in mongo_result:
            raise HTTPException(status_code=400, detail=f"MongoDB insert failed: {mongo_result['error']}")

        return {
            "message": f"{inserted} structured records saved",
            "file_uuid": file_uuid,
            "failures": failed_rows,
            "minio_metadata": minio_result
        }

    except Exception as e:
        logging.exception("Structured upload failed")
        raise HTTPException(status_code=500, detail="Structured upload failed")



# ------------------ EXPORT CDF DATA ------------------
@router.get("/cdf_data/")
def get_all_cdf_data(db: Session = Depends(get_db)):
    try:
        results = db.query(MaritimeDataCDF).all()
        response = []
        for row in results:
            source_name = db.query(Source).filter(Source.id == row.source_id).first()
            sub_source_name = db.query(SubSource).filter(SubSource.id == row.sub_source_id).first()
            location_wkt = None
            if row.location is not None:
                try:
                    location_wkt = to_shape(row.location).wkt
                except Exception:
                    location_wkt = "Invalid geometry"

            response.append({
                "id": row.id,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "speed": row.speed,
                "bearing": row.bearing,
                "course": row.course,
                "sys_trk_no": row.sys_trk_no,
                "source": source_name.name if source_name else None,
                "sub_source": sub_source_name.name if sub_source_name else None,
                "location": location_wkt,
                "raw_data": row.raw_data,
            })

        df = pd.DataFrame(response)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name="CDF Data")
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=cdf_export.xlsx"}
        )

    except Exception as e:
        logging.exception("Failed to fetch CDF data")
        raise HTTPException(status_code=500, detail="Failed to fetch data")

# ------------------ METADATA LIST ------------------
@router.get("/metadata/list")
def list_metadata(db: Session = Depends(get_db)):
    results = db.query(UploadMetadata).order_by(UploadMetadata.timestamp.desc()).all()
    return [
        {
            "id": m.id,
            "file_name": m.file_name,
            "source": m.source.name if m.source else None,
            "sub_source": m.sub_source.name if m.sub_source else None,
            "format": m.format,
            "record_count": m.record_count,
            "timestamp": m.timestamp
        }
        for m in results
    ]


# MINIO

FILE_TYPE_BUCKETS = {
    "image": "images",
    "pdf": "pdf",
    "video": "videos",
    "audio": "audio",
    "csv": "csv",
    "json": "json"
}

def get_file_category(mime_type):
    if mime_type.startswith("image/"):
        return "image"
    elif mime_type == "application/pdf":
        return "pdf"
    elif mime_type.startswith("video/"):
        return "video"
    elif mime_type.startswith("audio/"):
        return "audio"
    elif mime_type == "text/csv" or mime_type == "application/vnd.ms-excel":
        return "csv"
    elif mime_type == "application/json":
        return "json"
    return None


async def upload_file_to_minio(file: UploadFile):
    try:
        content_type = file.content_type
        file_type = get_file_category(content_type)

        # If unknown file type, derive bucket name from file extension
        if not file_type:
            ext = os.path.splitext(file.filename)[1].lstrip(".").lower()
            if ext:
                bucket_name = f"{ext}-files"
            else:
                bucket_name = "misc-files"
        else:
            bucket_name = FILE_TYPE_BUCKETS.get(file_type, f"{file_type}-files")

        # Create bucket if it doesn't exist
        if not MINIO_CLIENT.bucket_exists(bucket_name):
            MINIO_CLIENT.make_bucket(bucket_name)

        file_uuid = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        content = await file.read()

        file_stream = io.BytesIO(content)
        MINIO_CLIENT.put_object(
            bucket_name,
            file.filename,
            file_stream,
            length=len(content),
            content_type=content_type,
            metadata={
                "x-amz-meta-uuid": file_uuid,
                "x-amz-meta-timestamp": timestamp,
                "x-amz-meta-type": file_type or ext
            }
        )

        return {
            "filename": file.filename,
            "uuid": file_uuid,
            "timestamp": timestamp,
            "type": file_type or ext,
            "bucket": bucket_name,
            "content_type": content_type,
            "size": len(content)
        }

    except S3Error as e:
        return {"error": str(e)}

def store_metadata_in_mongodb(metadata: dict):
    try:
        result = METADATA_COLLECTION.insert_one(metadata)
        metadata.pop("_id", None)
        return metadata
    except Exception as e:
        return {"error": f"MongoDB insert failed: {str(e)}"}
    
