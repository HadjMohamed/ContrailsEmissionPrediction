import pandas as pd
import requests
from pathlib import Path
from io import StringIO
from dotenv import load_dotenv
import os

load_dotenv()
# Load environment variables
AIRPORT_URL=os.getenv("AIRPORT_URL","")
if not AIRPORT_URL: raise ValueError("AIRPORT_URL not set in .env file")

def extract_airports_data():
    """
    Extracts airports data from a remote CSV file (Github) and saves it locally as data/raw_data/airports_latest.csv 
    """
    response = requests.get(AIRPORT_URL)
    response.raise_for_status()  

    csv_data = StringIO(response.text)

    df = pd.read_csv(csv_data)

    df = df[df["iata_code"].notnull() & df["icao_code"].notnull()]
    
    # Saving to data folder 
    output_path = Path("data/raw_data/airports_latest.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
