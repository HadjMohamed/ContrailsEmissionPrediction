import pandas as pd

from contrails.config import IATA_COL, ICAO_COL

def merge_sources(
    flight_df: pd.DataFrame, 
    aircraft_df: pd.DataFrame, 
    airport_df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    Merges flight, aircraft_df and airport datasets 
    - Enriches aircraft_df_type with engine_model and other aircraft_df info
    - Maps IATA airport codes to OACI ones in order to be compatible with the model requirements

    Args:
        output_path (str): Path to save the merged CSV

    Returns:
        pd.DataFrame: Final merged DataFrame
    """
    df = pd.merge(
        flight_df,
        airport_df[[IATA_COL, ICAO_COL]],
        left_on="origin",
        right_on=IATA_COL,
        how="left"
    ).drop(columns=["origin", IATA_COL]).rename(columns={ICAO_COL: "origin"})

    df = pd.merge(
        df,
        airport_df[[IATA_COL, ICAO_COL]],
        left_on="destination",
        right_on=IATA_COL,
        how="left"
    ).drop(columns=["destination", IATA_COL]).rename(columns={ICAO_COL: "destination"})
    
    df = pd.merge(df, aircraft_df, on="aircraft_df_type", how="left")

    return df

