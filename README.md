# Vehicle Price Prediction ELT Pipeline with Airflow, dbt, and BigQuery

## Overview
This project implements an end-to-end ELT pipeline for vehicle price prediction dataset.  
It uses **Apache Airflow** for orchestration, **Google Cloud Storage (GCS)** and **BigQuery** for storage and processing, and **dbt** for data transformations.

The pipeline:
- Extracts raw data from Kaggle
- Loads it into BigQuery
- Transforms it into a star schema with dbt
- Publishes reporting views for downstream analytics

---

## Architecture

Kaggle → Airflow (EL DAG) → GCS → BigQuery (raw data)

↓

dbt (staging → marts → reporting)

↓

BigQuery analytics-ready views


---

## Project Structure

### Airflow DAGs (`/dags`)

- **`el-vehicle_price_prediction`** (Extract and Load pipeline)  
  - Downloads dataset from Kaggle  
  - Unzips and uploads to GCS  
  - Loads into BigQuery (`ready-de27.khaled_projects.Demo-2-raw_vehicle_price_prediction`)  

- **`dbt-vehicle_price_prediction`** (Transformation pipeline)  
  - Runs staging models (data cleaning, type casting, derived features)  
  - Runs marts (fact and dimension tables)  
  - Runs reporting models  

- **`master_dag_vehicles`** (Controller DAG)  
  - Triggers both pipelines in sequence:  
    `el-vehicle_price_prediction → dbt-vehicle_price_prediction`
  - And is able to trigger other dags in parallel if needed.
---

### dbt Models (`/dbt/my_dbt_project/models`)

- **Staging (`/staging/`)**  
  - `Demo-2-stg_vehicle_price_prediction.sql`  
    - Cleans and standardizes raw vehicle data  
    - Generates surrogate `vehicle_id`  
    - Normalizes text fields, casts numeric fields  
    - Derives features such as `mileage_per_year`, `price_per_mile`, and outlier flags  

- **Marts (`/marts/`)**  
  - Fact table: `Demo-2-fact_vehicleprices.sql`  
  - Dimension tables:  
    - `Demo-2-dim_vehicle.sql`  
    - `Demo-2-dim_vehicle_aesthetics.sql`  
    - `Demo-2-dim_vehicle_history.sql`  
    - `Demo-2-dim_vehicle_market.sql`  

- **Reporting (`/reporting/`)**  
  - `Demo-2-rpt_vehicleprice_analytics.sql`  
    - Combines fact and dimension tables into an analytics-ready view  

  


