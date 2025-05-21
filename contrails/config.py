from pathlib import Path
import os

from airflow.models import Variable # type: ignore

BASE_DATA_DIR=Variable.get("DATA_DIR")
RAW_DATA_DIR = os.path.join(BASE_DATA_DIR,"raw_data")
BASE_MODEL_DIR = Path(Variable.get("MODEL_DIR"), Path(__file__).resolve().parent.parent / "model")
MODEL_FILE="xgboost_contrails_model.json"
ENCODER_FILE="label_encoders.pkl"
FEATURE_FILE="model_features.pkl"

AIRPORTS_API=Variable.get("AIRPORTS_API")
AIRPORTS_URL = Variable.get("AIRPORTS_URL")
IATA_COL = "iata_code"
ICAO_COL = "icao_code"

COLUMN_MAPPING = {
    # Expected model column name : Column name in the merged CSV (value to adapt)
    "Aircraft": "aircraft_df_type",
    "Engine": "engine_model",
    "Seats": "nb_seats",
    "Origin Airport": "origin",
    "Destination Airport": "destination",
    "Distance Flown (km)": "distance_flown_km",
    "CO2 (kgCO2e)": "CO2_emission",
    "Date": "Date",
    "Take-off Time (UTC)": "Take-off Time (UTC)",
}


