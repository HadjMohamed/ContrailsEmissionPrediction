import pandas as pd
import requests
from io import StringIO
from datetime import datetime

from contrails.config import AIRPORTS_URL, IATA_COL, ICAO_COL, AIRPORTS_API

def get_airports_last_modified():
    """
    Gets the last modified date of the airports data from the Github API.
    Returns:
        datetime: The last modified date of the airports data.
    """
    params = {"path": "airports.csv", "per_page": 1}
    
    response = requests.get(AIRPORTS_API, params=params)
    response.raise_for_status()
    
    commit_data = response.json()[0]
    commit_date = commit_data["commit"]["committer"]["date"]
    
    commit_dt = datetime.fromisoformat(commit_date.replace("Z", "+00:00"))
    return commit_dt

def extract_airport():
    """
    Extracts airports data from a remote CSV file (on Github) and returns the DataFrame.
    Returns:
        pd.DataFrame: DataFrame containing the airports data.
    """
    if not AIRPORTS_URL:
        raise ValueError("AIRPORTS_URL is not set. Please set it in the .env file.")
    response = requests.get(AIRPORTS_URL)
    response.raise_for_status()  

    csv_data = StringIO(response.text)

    df = pd.read_csv(csv_data)

    if IATA_COL not in df.columns or ICAO_COL not in df.columns:
        raise ValueError(f"Missing required columns: {IATA_COL}, {ICAO_COL} in source file. Make sure they are defined in config file.")

    df = df[df[IATA_COL].notnull() & df[ICAO_COL].notnull()]

    return df
