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

        inserted = 0
        for line in lines:
            print(f"Processing line: {line}")
            if not line.strip():
                continue
            try:
                upload_uuid = str(uuid.uuid4())
                json_obj = json.loads(line)
                if upload_source.lower() == "piracy":
                    cdf_data = convert_to_piracy(json_obj,upload_uuid)
                elif upload_source.lower() == "ais":
                    cdf_data = convert_to_ais(json_obj,upload_uuid)
                elif upload_source.lower() == "dmas":
                    cdf_data = convert_to_cdf(json_obj,upload_uuid)
                elif upload_source.lower() == "p8i":
                    cdf_data=convert_to_cdf_from_csv_row(json_obj,upload_uuid)
            

                else:
                    print("source not found")

                db_data = MaritimeDataCDF(
                    **cdf_data,
                    source_id=source.id if source else 1,
                    sub_source_id=sub_source.id if sub_source else 1
                )
                db.add(db_data)
                db.commit()
                inserted += 1
            except Exception as e:
                logging.warning(f"Skipping line: {e}")

        # metadata_entry = UploadMetadata(
        #     file_name=file.filename,
        #     source_id=source.id if source else 1,
        #     sub_source_id=sub_source.id if sub_source else 1,
        #     format="NDJSON",
        #     record_count=inserted,
        #     type_of_data=upload_source
        # )
        # db.add(metadata_entry)
        # db.commit()

        # all_records = db.query(MaritimeDataCDF).all()
        # record_list = [{
        #     "latitude": r.latitude,
        #     "longitude": r.longitude,
        #     "speed": r.speed,
        #     "bearing": r.bearing,
        #     "course": r.course,
        #     "sys_trk_no": r.sys_trk_no,
        #     "raw_data": r.raw_data
        # } for r in all_records]

        # run_spark_etl(record_list)

        return {
            "message": f"{inserted} records saved and ETL executed successfully",
            "type_of_data": upload_source
        }

    except Exception as e:
        logging.exception("Failed to process file")
        raise HTTPException(status_code=500, detail="Failed to process file")


async def upload_structured(upload_source: str = Query(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        df = parse_structured_file(file)
        
        df.columns = df.columns.str.strip().str.replace("\u00a0", " ").str.replace("\t", " ").str.replace(r"\s+", " ", regex=True)

        print(f"Parsed DataFrame with columns: {upload_source}")

        source = db.query(Source).filter(func.lower(Source.name) == upload_source.lower()).first()
        sub_source = db.query(SubSource).first()

        inserted = 0
        failed_rows = []

        for i, row in df.iterrows():
            upload_uuid = str(uuid.uuid4())
            print(f"Generated Upload UUID: {upload_uuid}")
            try:
                if upload_source.lower() == "piracy":
                    cdf_data = convert_to_piracy(row,upload_uuid)
                elif upload_source.lower() == "ais":
                    cdf_data = convert_to_ais(row,upload_uuid)
                elif upload_source.lower() == "dmas":
                    cdf_data = convert_to_cdf(row,upload_uuid)
                elif upload_source.lower() == "p8i":
                    cdf_data=convert_to_cdf_from_csv_row(row,upload_uuid)
            

                else:
                    print("source not found")
    # fallback
             

                 #switch case to handle schema, identify source and call schema accordingly
               
                db_data = MaritimeDataCDF(
                    **cdf_data,
                    source_id=source.id if source else 1,
                    sub_source_id=sub_source.id if sub_source else 1
                    # optionally: add `upload_uuid=upload_uuid` here if MaritimeDataCDF has that field
                )
                db.add(db_data)
                inserted += 1
            except Exception as e:
                failed_rows.append(f"Row {i} failed: {e}")
                logging.warning(f"Row {i} failed: {e}")

        # metadata_entry = UploadMetadata(
        #     file_name=file.filename,
        #     source_id=source.id if source else 1,
        #     sub_source_id=sub_source.id if sub_source else 1,
        #     format="CSV",
        #     record_count=inserted,
        #     type_of_data=upload_source,
        #     upload_uuid=upload_uuid
        # )
        # db.add(metadata_entry)
        db.commit()


        # all_records = db.query(MaritimeDataCDF).all()
        # record_list = [{
        #     "latitude": r.latitude,
        #     "longitude": r.longitude,
        #     "speed": r.speed,
        #     "bearing": r.bearing,
        #     "course": r.course,
        #     "sys_trk_no": r.sys_trk_no,
        #     "raw_data": r.raw_data
        # } for r in all_records]

        # run_spark_etl(record_list)

        return {
            "message": f"{inserted} structured records saved and ETL executed successfully",
            "failures": failed_rows
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
