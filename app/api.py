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
from fastapi.responses import JSONResponse
from app.schemas import SubSourceOut


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

        elif content_type in ["text/csv", "application/vnd.ms-excel" , "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"] or filename.endswith(".csv"):
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
                    # uuid=record_uuid,
                    # file_uuid=file_uuid,
                    source_id=source.id if source else 1,
                    sub_source_id=sub_source.id if sub_source else 1
                )
                db.add(db_data)
                db.commit()
                inserted += 1
            except Exception as e:
                logging.warning(f"Skipping line due to error: {e}")

        minio_result = await upload_file_to_minio(file, file_uuid=str(file_uuid))
        if "error" in minio_result:
            raise HTTPException(status_code=400, detail=f"MinIO upload failed: {minio_result['error']}")

        if upload_source.isdigit():
            db = SessionLocal()
            source_obj = db.query(Source).filter(Source.id == int(upload_source)).first()
            source_name = source_obj.name if source_obj else "unknown"
            db.close()
        else:
            source_name = upload_source
        minio_result["source"] = source_name
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

        df = parse_structured_file(file_content, filename=file.filename)
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
                    # uuid=record_uuid,
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

        minio_result = await upload_file_to_minio(file, file_uuid=file_uuid)
        if "error" in minio_result:
            raise HTTPException(status_code=400, detail=f"MinIO upload failed: {minio_result['error']}")

        if upload_source.isdigit():
            db = SessionLocal()
            source_obj = db.query(Source).filter(Source.id == int(upload_source)).first()
            source_name = source_obj.name if source_obj else "unknown"
            db.close()
        else:
            source_name = upload_source

        minio_result["source"] = source_name
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
                "logged_timestamp": row.logged_timestamp.isoformat() if row.logged_timestamp else None,
                "vessel_name": row.vessel_name,
                "imo": row.imo,
                "mmsi": row.mmsi,   
                "uuid": str(row.uuid),
                "file_uuid": str(row.file_uuid),

                # "raw_data": row.raw_data,

            })

        df = pd.DataFrame(response)
        json_data = df.to_dict(orient="records")

# Return as JSON response
        return JSONResponse(content=json_data)
        # output = BytesIO()
        # with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        #     df.to_excel(writer, index=False, sheet_name="CDF Data")
        # output.seek(0)

        # return StreamingResponse(
        #     output,
        #     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        #     headers={"Content-Disposition": "attachment; filename=cdf_export.xlsx"}
        # )

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


async def upload_file_to_minio(file: UploadFile, file_uuid: str = None):
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

        file_uuid = file_uuid or str(uuid.uuid4())
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


  #28  
@router.get("/sources/")
def get_all_sources(db: Session = Depends(get_db)):
    try:
        sources = db.query(Source).all()
        return [
            {
                "id": source.id,
                "name": source.name
            }
            for source in sources
        ]
    except Exception as e:
        logging.exception("Failed to fetch source data")
        raise HTTPException(status_code=500, detail="Failed to fetch source data")




@router.get("/sub-sources/{source_id}", response_model=list[SubSourceOut])
def get_sub_sources(source_id: int, db: Session = Depends(get_db)):
    sub_sources = db.query(SubSource).filter(SubSource.source_id == source_id).all()
    if not sub_sources:
        raise HTTPException(status_code=404, detail="No sub-sources found for this source_id")
    return sub_sources


@router.get("/file-history")
def get_file_history():
    try:
        metadata_list = list(METADATA_COLLECTION.find({}, {'_id': 0}))  # Exclude Mongo's _id
        return JSONResponse(content=metadata_list)
    except Exception as e:
        logging.exception("Failed to fetch file history")
        raise HTTPException(status_code=500, detail="Failed to fetch file history")


@router.get("/file/raw-data/{uuid}")
def fetch_raw_data_by_uuid(uuid: str):
    try:
        # Search all buckets for the file with matching uuid
        for bucket in MINIO_CLIENT.list_buckets():
            bucket_name = bucket.name
            objects = MINIO_CLIENT.list_objects(bucket_name, recursive=True)

            for obj in objects:
                try:
                    stat = MINIO_CLIENT.stat_object(bucket_name, obj.object_name)
                    metadata = stat.metadata
                    if metadata.get("x-amz-meta-uuid") == uuid:
                        # Match found, fetch file content
                        response = MINIO_CLIENT.get_object(bucket_name, obj.object_name)
                        content = response.read()
                        response.close()
                        response.release_conn()

                        return StreamingResponse(
                            BytesIO(content),
                            media_type=stat.content_type,
                            headers={"Content-Disposition": f"attachment; filename={obj.object_name}"}
                        )
                except S3Error as e:
                    logging.warning(f"Error checking object {obj.object_name} in bucket {bucket_name}: {e}")
                    continue

        raise HTTPException(status_code=404, detail=f"No file found with uuid: {uuid}")

    except Exception as e:
        logging.exception("Failed to fetch raw data from MinIO")
        raise HTTPException(status_code=500, detail="Failed to fetch raw data from MinIO")




from fastapi.responses import StreamingResponse, JSONResponse

@router.get("/file/metadata/{uuid}")
def get_file_metadata(uuid: str, download: bool = False):
    try:
        # Fetch from MongoDB
        result = METADATA_COLLECTION.find_one({"uuid": uuid})
        if not result:
            raise HTTPException(status_code=404, detail="Metadata not found")

        result["_id"] = str(result["_id"])  # Convert ObjectId to str

        if download:
            # Extract required info
            bucket = result["bucket"]
            filename = result["filename"]
            content_type = result.get("content_type", "application/octet-stream")

            try:
                file_data = MINIO_CLIENT.get_object(bucket, filename)
                return StreamingResponse(
                    file_data,
                    media_type=content_type,
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'}
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to download: {str(e)}")

        # Return just metadata
        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# @router.get("/structured/cdf/download")
# def download_cdf_csv(db: Session = Depends(get_db)):
#     try:
#         records = db.query(MaritimeDataCDF).all()
#         data = [r.__dict__ for r in records]
        
#         for row in data:
#             row.pop('_sa_instance_state', None)

#         if not data:
#             raise HTTPException(status_code=404, detail="No CDF data found")

#         # Convert to CSV
#         df = pd.DataFrame(data)
#         output = io.StringIO()
#         df.to_csv(output, index=False)
#         output.seek(0)

#         return StreamingResponse(output, media_type='text/csv', headers={
#             "Content-Disposition": "attachment; filename=cdf_data.csv"
#         })
    
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     try:
#         # Query all structured CDF data
#         records = db.query(MaritimeDataCDF).all()

#         # Convert to DataFrame
#         data = [r.__dict__ for r in records]
#         for row in data:
#             row.pop('_sa_instance_state', None)  # Remove SQLAlchemy internal state

#         df = pd.DataFrame(data)

#         # Convert to Excel in-memory
#         output = io.BytesIO()
#         with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
#             df.to_excel(writer, index=False, sheet_name='CDF Data')
#         output.seek(0)

#         return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#                                  headers={"Content-Disposition": "attachment; filename=cdf_data.xlsx"})

#     except Exception as e:
#         return {"error": str(e)}
    


@router.get("/structured/cdf/download/{uuid}")
def download_cdf_csv(uuid: str, db: Session = Depends(get_db)):
    try:
        # Filter CDF records using file_uuid
        records = db.query(MaritimeDataCDF).filter(MaritimeDataCDF.file_uuid == uuid).all()

        if not records:
            raise HTTPException(status_code=404, detail="No CDF data found for this UUID")

        # Convert to list of dicts and remove SQLAlchemy state
        data = [r.__dict__ for r in records]
        for row in data:
            row.pop('_sa_instance_state', None)

        # Convert to CSV in memory
        df = pd.DataFrame(data)
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename=cdf_data_{uuid}.csv"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))