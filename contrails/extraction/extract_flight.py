import pandas as pd
from pathlib import Path

def extract_flight(path: str) -> pd.DataFrame:
    """
    Returns a DataFrame containing flight data.
    Args:
        path (str): Path to the CSV file containing flight data.
    Returns:
        pd.DataFrame: DataFrame containing flight data.
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found at : {path}")
    return pd.read_csv(path)