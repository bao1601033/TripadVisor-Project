CREATE OR REPLACE TABLE `capstone2-475108.capstone2_dataset.hotel_country_stats` AS
WITH lang_norm AS (
  SELECT
    CAST(hotel_id AS STRING) AS hotel_id,
    -- normalize language code
    CASE
      WHEN REGEXP_CONTAINS(LOWER(TRIM(language)), r'^[a-z]{1,4}$')
        THEN LOWER(TRIM(language))
      ELSE NULL
    END AS lang_code
  FROM `capstone2-475108.capstone2_dataset.hotel_reviews`
),

country_map AS (
  SELECT
    hotel_id,
    CASE
      WHEN lang_code = 'vi' THEN 'Vietnam'
      WHEN lang_code IN ('en','en-us','en-gb') THEN 'English-speaking'
      WHEN lang_code = 'ru' THEN 'Russia'
      WHEN lang_code = 'ko' THEN 'Korea'
      WHEN lang_code = 'ja' THEN 'Japan'
      WHEN lang_code IN ('zh','zh-cn','zh-tw') THEN 'China'
      WHEN lang_code = 'fr' THEN 'France'
      WHEN lang_code = 'de' THEN 'Germany'
      WHEN lang_code = 'es' THEN 'Spain'
      WHEN lang_code = 'it' THEN 'Italy'
      ELSE 'Other/Unknown'
    END AS country_label
  FROM lang_norm
  WHERE lang_code IS NOT NULL
)

SELECT
  hotel_id,
  country_label,
  COUNT(*) AS review_count
FROM country_map
GROUP BY hotel_id, country_label;
