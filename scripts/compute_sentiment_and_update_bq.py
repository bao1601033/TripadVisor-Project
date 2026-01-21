#!/usr/bin/env python3
"""
Compute sentiment for hotel reviews and update BigQuery tables:
- write sentiment_score into hotel_reviews (new column)
- aggregate by hotel_id and update tmp_hotel_metrics with average_sentiment, positive_ratio
"""

import re
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from langdetect import detect, LangDetectException

# CONFIG - chỉnh nếu cần
PROJECT = "capstone2-475108"
DATASET = "capstone2_dataset"
BQ = bigquery.Client(project=PROJECT)

# Lightweight Vietnamese lexicon sample (mở rộng nếu cần)
VI_POS = {
    "tốt": 1.0, "tuyệt": 1.2, "tuyệt vời": 1.5, "đẹp": 0.8, "thuận tiện": 0.8,
    "ngon": 0.7, "rõ ràng": 0.5, "dễ chịu": 1.0, "thoải mái": 1.0, "thân thiện": 1.2
}
VI_NEG = {
    "tệ": -1.0, "kém": -0.8, "dở": -0.7, "bẩn": -1.2, "đắt": -0.8,
    "không tốt": -1.0, "tồi": -1.2, "ồn": -0.6, "chậm": -0.5, "không hài lòng": -1.0
}

# init VADER
vader = SentimentIntensityAnalyzer()

def simple_vi_sentiment(text):
    """Very simple lexicon-based sentiment for Vietnamese text."""
    if not isinstance(text, str) or not text.strip():
        return 0.0
    txt = text.lower()
    # normalize punctuation
    txt = re.sub(r"[^\w\s]", " ", txt)
    score = 0.0
    tokens = txt.split()
    joined = " ".join(tokens)
    # check multiword positives first
    for phrase, val in VI_POS.items():
        if phrase in joined:
            score += val
    for phrase, val in VI_NEG.items():
        if phrase in joined:
            score += val
    # also check single tokens as backup
    for t in tokens:
        if t in VI_POS:
            score += VI_POS[t]
        if t in VI_NEG:
            score += VI_NEG[t]
    # normalize: map raw score to [-1,1]
    if score == 0:
        return 0.0
    # simple squash
    norm = max(min(score / 3.0, 1.0), -1.0)
    return round(norm, 3)

def detect_language_safe(text, fallback=None):
    if not isinstance(text, str) or not text.strip():
        return fallback or "unknown"
    try:
        lang = detect(text)
        return lang
    except LangDetectException:
        return fallback or "unknown"

def compute_sentiment_for_row(row):
    text = row.get("text") or ""
    lang_field = (row.get("language") or "").strip().lower()
    lang = lang_field if lang_field else detect_language_safe(text, fallback="en")
    # prefer explicit language column if present
    if lang.startswith("vi"):
        return simple_vi_sentiment(text)
    elif lang.startswith("en"):
        s = vader.polarity_scores(text)["compound"]
        return round(s, 3)
    else:
        # fallback: try detect
        detected = detect_language_safe(text, fallback="en")
        if detected.startswith("vi"):
            return simple_vi_sentiment(text)
        else:
            s = vader.polarity_scores(text)["compound"]
            return round(s, 3)

def fetch_reviews(limit=None, filter_null=False):
    sql = f"SELECT review_id, hotel_id, language, text, rating, created_date FROM `{PROJECT}.{DATASET}.hotel_reviews`"
    if filter_null:
        sql += " WHERE text IS NOT NULL"
    if limit:
        sql += f" LIMIT {limit}"
    df = BQ.query(sql).to_dataframe()
    print(f"Fetched {len(df)} reviews from BigQuery")
    return df

def upsert_sentiment_to_bq(df_sent):
    """Upsert sentiment_score back into hotel_reviews via temporary table + MERGE."""
    tmp_table = f"{PROJECT}.{DATASET}.tmp_reviews_sentiment"
    job = BQ.load_table_from_dataframe(df_sent, tmp_table, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()
    print("Uploaded sentiment temp table:", tmp_table)

    merge_sql = f"""
    MERGE `{PROJECT}.{DATASET}.hotel_reviews` T
    USING `{tmp_table}` S
    ON T.review_id = S.review_id
    WHEN MATCHED THEN
      UPDATE SET sentiment_score = S.sentiment_score, ingested_at = T.ingested_at
    """
    BQ.query(merge_sql).result()
    print("Merged sentiment_score into hotel_reviews")

def aggregate_and_update_metrics():
    """Compute average_sentiment and positive_ratio per hotel and update tmp_hotel_metrics."""
    agg_sql = f"""
    SELECT
      hotel_id,
      COUNT(1) AS review_count,
      AVG(rating) AS average_rating,
      AVG(sentiment_score) AS average_sentiment,
      SUM(CASE WHEN sentiment_score > 0.3 THEN 1 ELSE 0 END) / COUNT(1) AS positive_ratio
    FROM `{PROJECT}.{DATASET}.hotel_reviews`
    GROUP BY hotel_id
    """
    agg_df = BQ.query(agg_sql).to_dataframe()
    print("Aggregated metrics for", len(agg_df), "hotels")

    # load to tmp table and MERGE into tmp_hotel_metrics
    tmp_metrics = f"{PROJECT}.{DATASET}.tmp_hotel_metrics_new"
    BQ.load_table_from_dataframe(agg_df, tmp_metrics, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()

    merge_sql = f"""
    MERGE `{PROJECT}.{DATASET}.tmp_hotel_metrics` T
    USING `{tmp_metrics}` S
    ON T.hotel_id = S.hotel_id
    WHEN MATCHED THEN
      UPDATE SET review_count = S.review_count, average_rating = S.average_rating, average_sentiment = S.average_sentiment, positive_ratio = S.positive_ratio
    WHEN NOT MATCHED THEN
      INSERT (hotel_id, review_count, average_rating, average_sentiment, positive_ratio)
      VALUES (S.hotel_id, S.review_count, S.average_rating, S.average_sentiment, S.positive_ratio)
    """
    BQ.query(merge_sql).result()
    print("Updated tmp_hotel_metrics with sentiment metrics")

def ensure_tmp_metrics_schema():
    """Ensure tmp_hotel_metrics table has the extra columns (average_sentiment, positive_ratio)."""
    table_ref = f"{PROJECT}.{DATASET}.tmp_hotel_metrics"
    try:
        tbl = BQ.get_table(table_ref)
        # check fields
        cols = [f.name for f in tbl.schema]
        added = False
        if "average_sentiment" not in cols or "positive_ratio" not in cols:
            print("Altering table schema to add average_sentiment and positive_ratio (by recreating)...")
            # create new table with desired schema and copy old data
            create_sql = f"""
            CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.tmp_hotel_metrics` AS
            SELECT *, CAST(NULL AS FLOAT64) AS average_sentiment, CAST(NULL AS FLOAT64) AS positive_ratio
            FROM `{PROJECT}.{DATASET}.tmp_hotel_metrics`
            """
            BQ.query(create_sql).result()
            print("Schema updated.")
    except Exception as e:
        print("tmp_hotel_metrics table not found or error:", e)
        print("Creating tmp_hotel_metrics from scratch with sentiment columns...")
        create_sql = f"""
        CREATE TABLE `{PROJECT}.{DATASET}.tmp_hotel_metrics` AS
        SELECT hotel_id, 0 AS review_count, 0.0 AS average_rating, NULL AS average_sentiment, NULL AS positive_ratio
        FROM `{PROJECT}.{DATASET}.hotels_core` WHERE FALSE
        """
        BQ.query(create_sql).result()
        print("Created empty tmp_hotel_metrics table.")

def main(limit=None):
    ensure_tmp_metrics_schema()
    df = fetch_reviews(limit=limit, filter_null=True)
    if df.empty:
        print("No reviews found; abort.")
        return
    # compute sentiment per row
    df["sentiment_score"] = df.apply(lambda r: compute_sentiment_for_row(r), axis=1)
    # keep only necessary columns for upsert
    df_upsert = df[["review_id", "sentiment_score"]].copy()
    # upload and merge
    upsert_sentiment_to_bq(df_upsert)
    # aggregate and update metrics
    aggregate_and_update_metrics()
    print("All done at", datetime.utcnow().isoformat())

if __name__ == "__main__":
    # for testing, use a small limit e.g., limit=500
    main(limit=None)   # None -> process all reviews; set small number for test (e.g., 200)
