-- 1) Dataset create (run once)
CREATE SCHEMA IF NOT EXISTS `capstone2_capstone2_dataset`;

-- 2) hotels_core
CREATE OR REPLACE TABLE `capstone2_capstone2_dataset.hotels_core` (
  hotel_id STRING,            -- maps to locationId
  name STRING,
  parent_geo STRING,
  parent_geo_id STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  average_rating FLOAT64,
  review_count INT64,
  source STRING,
  created_at TIMESTAMP,
  last_updated TIMESTAMP
);

-- 3) hotel_reviews
CREATE OR REPLACE TABLE `capstone2_capstone2_dataset.hotel_reviews` (
  review_id STRING,
  hotel_id STRING,            -- FK to hotels_core.hotel_id
  user_id STRING,
  username STRING,
  language STRING,
  rating INT64,
  additional_ratings STRING,  
  created_date DATE,
  helpful_votes INT64,
  title STRING,
  text STRING,
  stay_date DATE,
  trip_type STRING,
  sentiment_score FLOAT64,
  ingested_at TIMESTAMP
);

-- 4) hotel_photos
CREATE OR REPLACE TABLE `capstone2_capstone2_dataset.hotel_photos` (
  photo_id STRING,
  hotel_id STRING,
  query_used STRING,
  result_rank INT64,
  url_regular STRING,
  url_thumb STRING,
  photographer_name STRING,
  photographer_username STRING,
  unsplash_page STRING,
  color STRING,
  likes INT64,
  fetched_at TIMESTAMP,
  is_fallback BOOL
);
