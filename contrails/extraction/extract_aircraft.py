import pandas as pd
from pathlib import Path

def extract_aircraft(path: str) -> pd.DataFrame:
    """
    Returns a DataFrame containing aircrafts data.
    Args:
        path (str): Path to the CSV file containing aircrafts data.
    Returns:
        pd.DataFrame: DataFrame containing aircrafts data.
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found at : {path}")
    return pd.read_csv(path_obj)
