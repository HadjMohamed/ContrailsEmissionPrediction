import pandas as pd
import numpy as np
import pytest

from scripts.preprocess import preprocess_df, validate_input
from scripts.config import COLUMN_MAPPING
from scripts.predictor import ContrailPredictor

def test_validate_input_success():
    data = {v: ["dummy"] for v in COLUMN_MAPPING.values()}
    df = pd.DataFrame(data)
    validate_input(df)  

def test_validate_input_failure():
    df = pd.DataFrame({})
    with pytest.raises(ValueError):
        validate_input(df)

def test_predictor():
    data = {
        "aircraft_type": ["A320"],
        "engine_model": ["V2527-A5"],
        "seats": [180],
        "distance_flown_km": [1000],
        "origin": ["LEAL"],
        "destination": ["EGKK"],
        "flight_date": ["2025-01-01"],
        "departure_time_utc": ["08:30"],
        "co2_emissions": [1234],
    }
    df_input = pd.DataFrame(data)

    predictor = ContrailPredictor()
    y_pred = predictor.predict_contrails(df_input)

    # Check shape and values
    assert isinstance(y_pred, (pd.Series, list, np.ndarray))
    assert len(y_pred) == len(df_input)
    assert not pd.isnull(y_pred).any()

