import pandas as pd
from pathlib import Path
import os

def merge_sources(
    flight_path: str, 
    aircraft_path: str, 
    airport_path: str, 
    output_path: str) -> pd.DataFrame:
    """
    Merges flight, aircraft_df and airport datasets into a single DataFrame ready for inference.
    - Enriches aircraft_df_type with engine_model and other aircraft_df info
    - Maps IATA airport codes to OACI ones in order to be compatible with the model requirements

    Args:
        output_path (str): Path to save the merged CSV

    Returns:
        pd.DataFrame: Final merged DataFrame
    """
    # Load raw data
    flights_df = pd.read_csv(flight_path)
    aircraft_df = pd.read_csv(aircraft_path)
    airports_df = pd.read_csv(airport_path)

    # Merge aircraft_df info via aircraft_df_type
    df = pd.merge(flights_df, aircraft_df, on="aircraft_df_type", how="left")

    # Merge origin OACI code
    df = pd.merge(
        df,
        airports_df[["code_iata", "code_oaci"]].rename(columns={
            "code_iata": "origin",
            "code_oaci": "origin_oaci"
        }),
        on="origin",
        how="left"
    )

    # Merge destination OACI code
    df = pd.merge(
        df,
        airports_df[["code_iata", "code_oaci"]].rename(columns={
            "code_iata": "destination",
            "code_oaci": "destination_oaci"
        }),
        on="destination",
        how="left"
    )

    # Replace origin/destination with OACI codes
    df["origin"] = df["origin_oaci"]
    df["destination"] = df["destination_oaci"]
    df.drop(columns=["origin_oaci", "destination_oaci"], inplace=True)

    # Saving
    df.to_csv(output_path, index=False)

    return df
