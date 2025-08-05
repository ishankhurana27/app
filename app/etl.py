from datetime import datetime, timedelta
import json
import logging
import math
import uuid
import pandas as pd
from io import BytesIO
from pathlib import Path


import pandas as pd



def convert_to_cdf(json_data: dict,upload_uuid: str,file_uuid: str) -> dict:
    ts = json_data.get("struct_update_track_timestamp", {})

    msg_date = f"{ts.get('date_dd', '00').zfill(2)}-{ts.get('date_mm', '00').zfill(2)}-{ts.get('date_yy', '0000')}"
    msg_time = f"{ts.get('time_hour', '00').zfill(2)}:{ts.get('time_minute', '00').zfill(2)}:{ts.get('time_second', '00')}"

    try:
        timestamp_str = convert_to_postgres_timestamp(msg_date, msg_time)
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError as e:
        logging.warning(f"Skipping line: Invalid date format: {msg_date}")
        return None

    return {
        "latitude": float(json_data["latitude"]),
        "longitude": float(json_data["longitude"]),
        "speed": float(json_data["speed"]),
        "bearing": float(json_data["bearing"]),
        "course": float(json_data["course"]),
        "logged_timestamp": timestamp,
        "sys_trk_no": int(json_data["sys_trk_no"]),
        "location": f'POINT({json_data["longitude"]} {json_data["latitude"]})',
        "raw_data": json_data,
        "uuid": upload_uuid,
        "file_uuid": file_uuid,

    }

def convert_to_cdf_from_csv_row(row: dict,upload_uuid: str) -> dict:
    timestamp = convert_to_postgres_timestamp(row["Position Valid Date YYYYMMDD"], row["Position Valid Time hh:mm:ss.sss"])
    return {
        "latitude": dms_to_decimal(row["Lat / Lat Origin"]),
        "longitude": dms_to_decimal(row["Long / Long Origin"]),
        "speed": float(row["Speed Kts / Spare"]),
        "bearing": float(row.get("Course Deg / Bearing Deg", 0)),
        "course": float(row.get("Course Deg / Bearing Deg", 0)),
        "logged_timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f"),
        "raw_data": dict(row),
        "uuid": upload_uuid
    }
def convert_to_piracy(row: dict, upload_uuid: str) -> dict:
  
    timestamp = convert_to_postgres_timestamp(
        row["date"],
        row["time"]
    )
    
    return {
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "logged_timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f"),
        "raw_data": json.dumps(clean_nan(row.to_dict())),
        "uuid": upload_uuid,
        "vessel_name": str(row.get("vessel_name", "")).strip()
    }


def clean_nan(obj):
    """
    Recursively convert float('nan') → None for dicts/lists
    """
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

# def convert_to_piracy(row: dict,upload_uuid: str) -> dict:
#     latitude = float(row["latitude"])
#     longitude = float(row["longitude"])
#     return {
#         "location": f'POINT({longitude} {latitude})',
#         "datetime": f"{row.get('date', '').strip()} {row.get('time', '').strip()}",
#         "attack_description": row.get("attack_description", "").strip(),
#         "vessel_name": row.get("vessel_name", "").strip(),
#         "vessel_type": row.get("vessel_type", "").strip(),
#         "vessel_status": row.get("vessel_status", "").strip(),
#         "timestamp": row.get("Timestamp", "").strip(),
#         "raw_data": dict(row),
#         "uuid": upload_uuid
#     }

# def convert_to_ais(row: dict,upload_uuid: str) -> dict:
#     return {
#         "timestamp": row.get("Timestamp", row.get("BaseDateTime", "")).strip(),
#         "vessel_name": row.get("VesselName", "").strip(),
#         "imo": row.get("IMO", "").strip(),
#         "raw_data": dict(row),
#         "uuid": upload_uuid,
#         "mmsi": row.get("MMSI", "").strip(),
#         "latitude": dms_to_decimal(row["Lat / Lat Origin"]),
#         "longitude": dms_to_decimal(row["Lon / Lon Origin"]),
#         "speed": float(row["SOG / Spare"]), #Speed Kts
#         "course": float(row.get("COG / Bearing Deg", 0)), #Course Deg 
#     }

def convert_to_ais(row: dict, upload_uuid: str) -> dict:
    return {
        "logged_timestamp": str(row.get("Timestamp", row.get("BaseDateTime", ""))).strip(),
        "vessel_name": str(row.get("VesselName", "")).strip(),
        "imo": str(row.get("IMO", "")).strip(),
        "mmsi": str(row.get("MMSI", "")).strip(),
        "latitude":(row.get("LAT")),
        "longitude":(row.get("LON")),
        "speed": float(row.get("SOG")),
        "course": float(row.get("Heading")),
        "raw_data": json.dumps(clean_nan(row.to_dict())),
        "uuid": upload_uuid
    }




# def convert_to_postgres_timestamp(msg_date: str, msg_time: str) -> str:
    
#     print(msg_date,msg_time)
#     msg_date = str(msg_date)
#     msg_time=str(msg_time)
#     print(type(msg_date),type(msg_time))
#     date_obj = datetime.strptime(msg_date, "%Y%m%d").date()

#     parts = msg_time.split(":")
#     if len(parts) == 2:
#         hours = 0
#         minutes = int(parts[0])
#         seconds = float(parts[1])
#     elif len(parts) == 3:
#         hours = int(parts[0])
#         minutes = int(parts[1])
#         seconds = float(parts[2])
#     else:
#         raise ValueError(f"Invalid time format: {msg_time}")

#     secs_int = int(seconds)
#     micros = int((seconds - secs_int) * 1_000_000)
#     time_delta = timedelta(
#         hours=hours,
#         minutes=minutes,
#         seconds=secs_int,
#         microseconds=micros
#     )

#     full_dt = datetime.combine(date_obj, datetime.min.time()) + time_delta

#     return full_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
def convert_to_postgres_timestamp(msg_date: str, msg_time: str) -> str:
    from datetime import datetime, timedelta

    msg_date = str(msg_date).strip().split(".")[0]
    msg_time = str(msg_time).strip().upper()

    try:
        if "-" in msg_date:
            date_obj = datetime.strptime(msg_date, "%d-%m-%Y").date()
        elif len(msg_date) == 8 and msg_date.isdigit():
            date_obj = datetime.strptime(msg_date, "%Y%m%d").date()
        else:
            raise ValueError
    except ValueError as e:
        raise ValueError(f"Invalid date format: {msg_date}") from e

    if not msg_time or msg_time in {"NA", "NAN"}:
        minutes = seconds = 0
    else:
        msg_time = msg_time.replace("UTC", "").replace("GMT", "").strip()
        if "," in msg_time:
            msg_time = msg_time.split(",")[0].strip()

        try:
            parts = msg_time.split(":")
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = float(parts[1])
            elif len(parts) == 3:
                # Original expected format
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2])
                secs_int = int(seconds)
                micros = int((seconds - secs_int) * 1_000_000)
                time_delta = timedelta(
                    hours=hours,
                    minutes=minutes,
                    seconds=secs_int,
                    microseconds=micros
                )
                full_dt = datetime.combine(date_obj, datetime.min.time()) + time_delta
                return full_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            else:
                raise ValueError
        except ValueError:
            raise ValueError(f"Invalid time format: {msg_time}")

        secs_int = int(seconds)
        micros = int((seconds - secs_int) * 1_000_000)
        time_delta = timedelta(
            minutes=minutes,
            seconds=secs_int,
            microseconds=micros
        )

    full_dt = datetime.combine(date_obj, datetime.min.time()) + time_delta
    return full_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

# def parse_structured_file(file) -> pd.DataFrame:
#     filename = file.filename.lower()
#     file.file.seek(0)
#     if filename.endswith(".csv"):
#         return pd.read_csv(BytesIO(file.file.read()))
#     elif filename.endswith(".tsv"):
#         return pd.read_csv(BytesIO(file.file.read()), sep="\t")
#     elif filename.endswith(".xlsx"):
#         return pd.read_excel(BytesIO(file.file.read()))
#     else:
#         raise ValueError("Unsupported file format")

def parse_structured_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    filename = filename.lower()
    if filename.endswith(".csv"):
        return pd.read_csv(BytesIO(file_bytes))
    elif filename.endswith(".tsv"):
        return pd.read_csv(BytesIO(file_bytes), sep="\t")
    elif filename.endswith(".xlsx"):
        return pd.read_excel(BytesIO(file_bytes))
    else:
        raise ValueError("Unsupported file format")

    
def dms_to_decimal(coord_str: str) -> float:
    try:
        dms, direction = coord_str.strip().split()
        degrees, minutes, seconds = map(float, dms.split(":"))
        decimal = degrees + minutes / 60 + seconds / 3600
        if direction in ("S", "W"):
            decimal *= -1
        return round(decimal, 6)
    except Exception as e:
        raise ValueError(f"Invalid coordinate format: {coord_str}")

tracking_sources = {"Redicent", "Efficnt", "Esmcnt", "Eoircnt", "Ardrcnt"}
esm_sources = {"Esm_emit beams", "Esm emitters"}


from datetime import datetime
from typing import Dict, Any

def convert_to_p8i_tracking(row: Dict[str, Any], upload_uuid: str) -> Dict[str, Any]:
    def safe_parse_date(value: str, fmt: str) -> datetime.date:
        try:
            return datetime.strptime(value.strip(), fmt).date() if value else None
        except Exception:
            raise ValueError(f"Invalid date format: {value}")

    def safe_parse_time(value: str, fmt: str) -> datetime.time:
        try:
            return datetime.strptime(value.strip(), fmt).time() if value else None
        except Exception:
            raise ValueError(f"Invalid time format: {value}")

    # Normalize keys to avoid casing or spacing issues
    row = {k.strip().lower(): v for k, v in row.items()}

    return {
        "msg_date": safe_parse_date(row.get("msg_date"), "%d-%m-%Y"),
        "msg_time": safe_parse_time(row.get("msg_time"), "%H:%M:%S"),
        "change_type": row.get("change_type"),
        "latitude": float(row.get("latitude", 0)),
        "longitude": float(row.get("longitude", 0)),
        "altitude": float(row.get("altitude", 0)),
        "course": float(row.get("course", 0)),
        "bearing": float(row.get("bearing", 0)),
        "speed": float(row.get("speed", 0)),
        "position_valid_date": safe_parse_date(
            row.get("position valid date yyyymmdd") or row.get("position_valid_date"),
            "%Y%m%d" if row.get("position valid date yyyymmdd") else "%d-%m-%Y"
        ),
        "position_valid_time": safe_parse_time(row.get("position_valid_time"), "%H:%M:%S"),
        "trk_short_name": row.get("trk_short_name"),
        "sub_source_name": row.get("sub_source_name"),
        "file_uuid": upload_uuid,
    }


import re
from datetime import datetime
from typing import Dict, Any

def convert_to_p8i_esm(row: Dict[str, Any], upload_uuid: str) -> Dict[str, Any]:
    def safe_parse_date(value: str, fmt: str) -> datetime.date:
        try:
            return datetime.strptime(value.strip(), fmt).date() if value else None
        except Exception:
            raise ValueError(f"Invalid date format: {value}")

    def safe_parse_time(value: str, fmt: str) -> datetime.time:
        try:
            return datetime.strptime(value.strip(), fmt).time() if value else None
        except Exception:
            raise ValueError(f"Invalid time format: {value}")

    # Normalize + clean headers
    row = {
        re.sub(r"\s+", " ", k).strip().lower().replace("\xa0", " "): v
        for k, v in row.items()
    }

    pos_date = (
        row.get("position valid date yyyymmdd")
        or row.get("position_valid_date")
        or row.get("date")
    )
    pos_time = (
        row.get("position valid time")
        or row.get("position_valid_time")
        or row.get("time")
    )

    if not pos_date:
        raise ValueError("Missing required field: Position Valid Date YYYYMMDD")

    return {
        "msg_date": safe_parse_date(row.get("msg_date"), "%d-%m-%Y"),
        "msg_time": safe_parse_time(row.get("msg_time"), "%H:%M:%S"),
        "emitter": row.get("emitter"),
        "emitter_enumeration": row.get("emitter_enumeration"),
        "beam_enumeration": row.get("beam_enumeration"),
        "change_type": row.get("change_type"),
        "amplitude": float(row.get("amplitude", 0) or 0),
        "frequency": float(row.get("frequency", 0) or 0),

        # 🔑 Return with the exact expected key
        "Position Valid Date YYYYMMDD": (
            safe_parse_date(pos_date, "%Y%m%d")
            if row.get("position valid date yyyymmdd")
            else safe_parse_date(pos_date, "%d-%m-%Y")
        ),
        "Position Valid Time": safe_parse_time(pos_time, "%H:%M:%S"),

        "last_beam_intercept_date": safe_parse_date(row.get("last_beam_intercept_date"), "%d-%m-%Y"),
        "last_beam_intercept_time": safe_parse_time(row.get("last_beam_intercept_time"), "%H:%M:%S"),

        "azimuth_degree": float(row.get("azimuth_degree", 0) or 0),
        "latitude": float(row.get("latitude", 0) or 0),
        "longitude": float(row.get("longitude", 0) or 0),
        "azimuth_accuracy_degree": float(row.get("azimuth_accuracy_degree", 0) or 0),
        "sub_source_name": row.get("sub_source_name"),
        "file_uuid": upload_uuid,
    }






from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os

def save_data_to_spark(spark: SparkSession, schema: StructType, rows: list[dict], upload_uuid: str, file_name: str):
    if not rows:
        raise ValueError("No data to process.")

    sub_source = rows[0].get("sub_source_name")
    if not sub_source:
        raise ValueError("Missing 'sub_source_name' in row.")

    if sub_source in tracking_sources:
        schema_name = "p8i_tracking"
        table_name = "tracking_data"
    elif sub_source in esm_sources:
        schema_name = "p8i_esm"
        table_name = "esm_data"
    else:
        raise ValueError(f"Unknown sub_source: {sub_source}")

    df = spark.createDataFrame(rows, schema=schema)

    df.write \
        .format("jdbc") \
        .option("url", os.getenv("DATABASE_URL")) \
        .option("dbtable", f"{schema_name}.{table_name}") \
        .option("user", os.getenv("DATABASE_USER")) \
        .option("password", os.getenv("DATABASE_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"✅ Data saved to {schema_name}.{table_name}")
