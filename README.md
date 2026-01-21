# Vietnam Hotel Review Intelligence Platform

## Overview
This project builds an end-to-end data pipeline to collect, process, enrich, and analyze hotel reviews in Vietnam, using Google BigQuery as the Data Warehouse and Looker Studio for visualization.

The system helps tourists easily explore hotels based on location, ratings, sentiment, review behavior, and visual information.
## Looker Studio Dashboard

Below is a snapshot of the interactive dashboard built on top of BigQuery Data Mart:

![Hotel Analytics Dashboard](docs/looker_dashboard_overview.png)

🔗 **Live dashboard**:  
[https://lookerstudio.google.com/reporting/XXXXXXX](https://lookerstudio.google.com/reporting/cdcdace5-137d-4ba0-9e19-9a29ac5aea36)


## Tech Stack
- Data Warehouse: Google BigQuery
- ETL & Processing: Python, BigQuery SQL
- Infrastructure: Google Cloud Shell
- Visualization: Looker Studio
- External API: Unsplash API
- NLP: Sentiment Analysis (rule-based / pretrained model)

## Data Pipeline
1. Data Collection from Tripadvisor (Excel files)
2. Data Cleaning & Loading into BigQuery
3. Metric Aggregation (ratings, sentiment)
4. Image Enrichment via Unsplash API
5. Country Inference from Review Language
6. Data Mart Creation for BI
7. Visualization with Looker Studio

## BigQuery Data Model
- hotels_core
- hotel_reviews
- hotel_photos
- tmp_hotel_metrics
- hotel_dashboard_view (Data Mart)

## Data Mart
The `hotel_dashboard_view` serves as a Data Mart optimized for BI tools, reducing join complexity and improving query performance in Looker Studio.

## Dashboard
The dashboard enables users to:
- Search hotels by location
- Compare ratings and sentiment
- Explore traveler origin by language
- View hotel images
- Analyze review behavior over time

## How to Run
1. Create tables using SQL scripts in `/sql`
2. Load data via Cloud Shell
3. Run sentiment pipeline
4. Enrich images using Unsplash
5. Connect Looker Studio to `hotel_dashboard_view`

## Notes
- API keys are not included
- Dataset is anonymized
