#!/usr/bin/env python3
import os
import time
import requests
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
UNSPLASH_KEY = os.getenv("UNSPLASH_ACCESS_KEY")
PROJECT = "capstone2-475108"   # sửa nếu cần
DATASET = "capstone2_dataset"
BQ_CLIENT = bigquery.Client(project=PROJECT)
SEARCH_URL = "https://api.unsplash.com/search/photos"

def search_unsplash(query, per_page=3):
    if not UNSPLASH_KEY:
        print("❌ UNSPLASH_ACCESS_KEY not set. Please add it to .env or export environment variable.")
        return []
    params = {"query": query, "per_page": per_page, "client_id": UNSPLASH_KEY}
    try:
        r = requests.get(SEARCH_URL, params=params, timeout=10)
    except Exception as e:
        print("❌ Request failed:", e)
        return []
    if r.status_code == 200:
        return r.json().get("results", [])
    else:
        print(f"❌ Unsplash API error {r.status_code}: {r.text}")
        return []

def get_hotels(limit=20):
    q = f"""
    SELECT hotel_id, name, parent_geo
    FROM `{PROJECT}.{DATASET}.hotels_core`
    WHERE hotel_id IS NOT NULL AND name IS NOT NULL
    LIMIT {limit}
    """
    rows = list(BQ_CLIENT.query(q).result())
    print(f"ℹ️ Fetched {len(rows)} hotels from BigQuery (limit={limit})")
    return rows

def insert_photos(rows):
    if not rows:
        print("ℹ️ No photo rows to insert — skipping BigQuery insert.")
        return
    table_id = f"{PROJECT}.{DATASET}.hotel_photos"
    print(f"ℹ️ Inserting {len(rows)} rows into {table_id} ...")
    errors = BQ_CLIENT.insert_rows_json(table_id, rows)
    if errors:
        print("❌ Insert errors:", errors)
    else:
        print("✅ Insert completed.")

def enrich_hotels(limit=20, test_mode=False):
    hotels = get_hotels(limit)
    if not hotels:
        print("⚠️ No hotels to process. Check hotels_core table.")
        return

    rows_to_insert = []
    # If test_mode=True, only process the first hotel and limit results
    hotels_iter = hotels[:1] if test_mode else hotels

    for h in tqdm(hotels_iter, desc="🔍 Finding images"):
        hid = h.hotel_id
        name = h.name
        city = h.parent_geo or ""
        queries = [f"{name} {city}".strip(), f"{name}".strip(), f"{city} hotel".strip(), "hotel interior"]
        found_for_hotel = False
        for q in queries:
            results = search_unsplash(q, per_page=3)
            time.sleep(0.35)
            if results:
                print(f"✔️ Found {len(results)} results for query: '{q}' (hotel_id={hid})")
                for idx, r in enumerate(results):
                    rows_to_insert.append({
                        "photo_id": r.get("id"),
                        "hotel_id": hid,
                        "query_used": q,
                        "result_rank": idx + 1,
                        "url_regular": r.get("urls", {}).get("regular"),
                        "url_thumb": r.get("urls", {}).get("thumb"),
                        "photographer_name": (r.get("user") or {}).get("name"),
                        "photographer_username": (r.get("user") or {}).get("username"),
                        "unsplash_page": r.get("links", {}).get("html"),
                        "color": r.get("color"),
                        "likes": r.get("likes", 0),
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "is_fallback": q not in [queries[0], queries[1]]
                    })
                found_for_hotel = True
                break
            else:
                print(f"ℹ️ No results for query: '{q}'")
        if not found_for_hotel:
            print(f"⚠️ No images found for hotel_id={hid}, name='{name}'")

    # finally insert if we have rows
    insert_photos(rows_to_insert)
    print("🎉 Enrichment finished.")

if __name__ == "__main__":
    # Run in test mode first (process only 1 hotel) to verify everything works.
    # To run full batch, call enrich_hotels(limit=100, test_mode=False)
    enrich_hotels(limit=20, test_mode=True)
