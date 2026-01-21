CREATE OR REPLACE VIEW `capstone2-475108.capstone2_dataset.hotel_dashboard_view` AS
WITH photo_one AS (
  SELECT
    hotel_id AS photo_hotel_id,
    (ARRAY_AGG(url_regular IGNORE NULLS ORDER BY COALESCE(likes, 0) DESC LIMIT 1))[OFFSET(0)] AS photo_url
  FROM `capstone2-475108.capstone2_dataset.hotel_photos`
  GROUP BY hotel_id
),
metrics_str AS (
  SELECT
    CAST(hotel_id AS STRING) AS hotel_id_str,
    review_count,
    average_rating,
    average_sentiment,
    positive_ratio
  FROM `capstone2-475108.capstone2_dataset.tmp_hotel_metrics`
),
city_avg AS (
  SELECT
    h.parent_geo AS city,
    AVG(m.average_rating) AS city_avg_rating,
    AVG(m.average_sentiment) AS city_avg_sentiment,
    AVG(m.positive_ratio) AS city_avg_positive_ratio
  FROM `capstone2-475108.capstone2_dataset.hotels_core` h
  LEFT JOIN metrics_str m
    ON CAST(h.hotel_id AS STRING) = m.hotel_id_str
  GROUP BY h.parent_geo
)
SELECT
  CAST(h.hotel_id AS STRING) AS hotel_id,
  h.name AS hotel_name,
  h.parent_geo AS city,
  CAST(h.parent_geo_id AS STRING) AS parent_geo_id,
  
  SAFE_CAST(h.latitude AS FLOAT64) AS latitude,
  SAFE_CAST(h.longitude AS FLOAT64) AS longitude,

  CONCAT(CAST(ROUND(SAFE_CAST(h.latitude AS FLOAT64),6) AS STRING), ",",
         CAST(ROUND(SAFE_CAST(h.longitude AS FLOAT64),6) AS STRING)) AS lat_long,

  COALESCE(m.review_count, 0) AS review_count,
  
  -- Lấp NULL bằng trung bình thành phố
  COALESCE(m.average_rating, c.city_avg_rating) AS average_rating_filled,
  COALESCE(m.average_sentiment, c.city_avg_sentiment) AS average_sentiment_filled,
  COALESCE(m.positive_ratio, c.city_avg_positive_ratio) AS positive_ratio_filled,

  -- Phần trăm
  ROUND(COALESCE(m.positive_ratio, c.city_avg_positive_ratio) * 100, 1) AS positive_pct,
  
  p.photo_url,

  CASE
    WHEN COALESCE(m.average_rating, c.city_avg_rating) >= 4.5 THEN '★★★★★'
    WHEN COALESCE(m.average_rating, c.city_avg_rating) >= 4.0 THEN '★★★★'
    WHEN COALESCE(m.average_rating, c.city_avg_rating) >= 3.0 THEN '★★★'
    WHEN COALESCE(m.average_rating, c.city_avg_rating) >= 2.0 THEN '★★'
    ELSE '★'
  END AS rating_stars,

  CASE
    WHEN COALESCE(m.average_sentiment, c.city_avg_sentiment) > 0.3 THEN 'Positive'
    WHEN COALESCE(m.average_sentiment, c.city_avg_sentiment) < -0.3 THEN 'Negative'
    ELSE 'Neutral'
  END AS sentiment_label

FROM `capstone2-475108.capstone2_dataset.hotels_core` h
LEFT JOIN metrics_str m
  ON CAST(h.hotel_id AS STRING) = m.hotel_id_str
LEFT JOIN photo_one p
  ON CAST(h.hotel_id AS STRING) = p.photo_hotel_id
LEFT JOIN city_avg c
  ON h.parent_geo = c.city;
