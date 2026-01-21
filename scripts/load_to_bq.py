from datetime import datetime
import pandas as pd
from google.cloud import bigquery

PROJECT = "capstone2-475108"
DATASET = "capstone2_dataset"
HOTEL_XLSX = "HotelCoodinate.xlsx"
REVIEWS_XLSX = "Reviews_fixed.xlsx"

client = bigquery.Client(project=PROJECT)

def ensure_dataset():
    ds_ref = client.dataset(DATASET)
    try:
        client.get_dataset(ds_ref)
        print(f"✅ Dataset '{DATASET}' đã tồn tại.")
    except Exception:
        ds = bigquery.Dataset(ds_ref)
        ds.location = "US"
        client.create_dataset(ds)
        print(f"🆕 Đã tạo dataset '{DATASET}'.")

def load_hotels():
    df = pd.read_excel(HOTEL_XLSX, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]

    df = df.rename(columns={
        "locationId": "hotel_id",
        "name": "name",
        "parentGeo": "parent_geo",
        "parentGeoId": "parent_geo_id",
        "latitude": "latitude",
        "longitude": "longitude"
    })

    now = datetime.utcnow()
    df["source"] = "excel"
    df["created_at"] = now
    df["last_updated"] = now

    table_id = f"{PROJECT}.{DATASET}.hotels_core"
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print(f"🏨 Đã nạp {job.output_rows} bản ghi vào 'hotels_core'.")

def load_reviews_and_compute_metrics():
    """Nạp review và tính trung bình rating."""
    df = pd.read_excel(REVIEWS_XLSX, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]

    df = df.rename(columns={
        "id": "review_id",
        "locationId": "hotel_id",
        "userId": "user_id",
        "username": "username",
        "language": "language",
        "rating": "rating",
        "additionalRatings": "additional_ratings",
        "createdDate": "created_date",
        "helpfulVotes": "helpful_votes",
        "title": "title",
        "text": "text",
        "stayDate": "stay_date",
        "tripType": "trip_type"
    })

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df["stay_date"] = pd.to_datetime(df["stay_date"], errors="coerce")
    df["ingested_at"] = datetime.utcnow()

    table_id = f"{PROJECT}.{DATASET}.hotel_reviews"
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print(f" Đã nạp {job.output_rows} bản ghi vào 'hotel_reviews'.")

    #  Tính trung bình rating và số lượng review mỗi khách sạn
    agg = df.groupby("hotel_id").agg(
        review_count=("review_id", "count"),
        average_rating=("rating", "mean")
    ).reset_index()
    agg["average_rating"] = agg["average_rating"].round(2)

    tmp_table = f"{PROJECT}.{DATASET}.tmp_hotel_metrics"
    client.load_table_from_dataframe(
        agg,
        tmp_table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()

    #  Cập nhật lại bảng hotels_core bằng MERGE
    merge_sql = f"""
    MERGE `{PROJECT}.{DATASET}.hotels_core` T
    USING `{tmp_table}` S
    ON T.hotel_id = S.hotel_id
    WHEN MATCHED THEN
      UPDATE SET
        average_rating = S.average_rating,
        review_count = S.review_count,
        last_updated = CURRENT_TIMESTAMP()
    """
    client.query(merge_sql).result()
    print(" Đã cập nhật 'hotels_core' với trung bình rating và số lượng review.")

if __name__ == "__main__":
    ensure_dataset()
    load_hotels()
    load_reviews_and_compute_metrics()
    print(" Hoàn tất nạp dữ liệu lên BigQuery!")
