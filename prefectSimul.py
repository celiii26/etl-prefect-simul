# Prerequisite Modules
import requests
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import datetime
from prefect import flow, task # pip install prefect

# Configure Key and Credentials Needed
LASTFM_API_KEY="<YOUR_API_KEY>"
GOOGLE_APPLICATION_CREDENTIALS="<YOUR_JSON_CREDENTIAL_FILE_PATH>"
# Setup credentials
scopes = ['https://www.googleapis.com/auth/bigquery']
credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS, scopes=scopes)

# --------------------------
# 1. TASKS
# --------------------------

@task(retries=3, retry_delay_seconds=10)
def extract_lastfm(api_key: str, artist: str, limit: int = 20) -> pd.DataFrame:
    """Extract top tracks for a given artist from Last.fm API"""
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "artist.gettoptracks",
        "artist": artist,
        "api_key": api_key,
        "format": "json",
        "limit": limit
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()["toptracks"]["track"]
    
    # Convert to DataFrame
    df = pd.DataFrame([{
        "artist": t["artist"]["name"],
        "track_name": t["name"],
        "playcount": int(t["playcount"]),
        "listeners": int(t["listeners"]),
        "url": t["url"],
        "rank": int(t["@attr"]["rank"]),
        "retrieved_at": datetime.datetime.now()
    } for t in data])
    
    return df


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform the raw Last.fm data"""
    # Example transform: normalize text
    df["artist"] = df["artist"].str.title()
    df["track_name"] = df["track_name"].str.strip()
    
    # Keep only top 10 for demo
    df = df.sort_values(by="playcount", ascending=False).head(10)
    
    return df


@task
def load_to_bigquery(df: pd.DataFrame, project_id: str, dataset: str, table: str):
    """Load the transformed DataFrame into BigQuery"""
    pandas_gbq.to_gbq(dataframe=df,
                      destination_table=f'{dataset}.{table}',
                      project_id=project_id,
                      if_exists='replace',
                      credentials=credentials)
    print(f"Loaded {len(df)} rows into {table}.")

# --------------------------
# 2. FLOW
# --------------------------

@flow(name="LastFM ETL Flow")
def etl_flow():
    api_key = LASTFM_API_KEY
    project_id = "<YOUR_PROJECT_ID>" # get it from bigquery
    dataset = "music_data"
    table = "lastfm_top_tracks"
    
    raw = extract_lastfm(api_key, artist="NCT")
    clean = transform_data(raw)
    load_to_bigquery(clean, project_id, dataset, table)

# --------------------------
# RUN THE APP
# --------------------------
if __name__ == "__main__":
    etl_flow()

etl_flow.serve(
    name="lastfm-etl-daily",
    cron="0 0 * * *" # run daily at midnight 

)

