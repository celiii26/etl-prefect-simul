# Automatic ETL Simulation using Prefect, BigQuery, and Last.fm API
This repository contains an example of an automated ETL pipeline built with Prefect for orchestration and Google BigQuery as the data warehouse. The pipeline extracts music track data from the Last.fm API, applies simple transformations, and loads the cleaned data into BigQuery for further analysis.

## 📌 Project Overview
- Extract: Fetch top tracks for a given artist from the Last.fm API.
- Transform: Clean and filter the raw data (keep top 10 tracks, normalize text).
- Load: Store the transformed data into a BigQuery table.
- Orchestrate: Automate the pipeline with Prefect flows, tasks, retries, and scheduling.

## ⚙️ Requirements
- Python 3.8+
- Prefect: `pip install prefect`
- Google Cloud Project with BigQuery enabled
- Last.fm API key

## 🔑 Setup
- Get a Last.fm API key from Last.fm API Docs
- Create a GCP Service Account Key with BigQuery permissions and save the JSON locally.
- Update the following variables in the script:
  ```
  LASTFM_API_KEY = "your_lastfm_api_key"
  GOOGLE_APPLICATION_CREDENTIALS = "path/to/your-service-account.json"
  ```

## ▶️ Running the Pipeline
Run the ETL flow locally:
```
python etl_prefect_lastfm.py
```
Start the Prefect UI and agent for scheduling:
```
python -m prefect server start
```
You can monitor the runs via localhost Prefect UI at `http://127.0.0.1:4200`.
