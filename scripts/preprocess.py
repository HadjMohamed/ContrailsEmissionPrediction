from sklearn.preprocessing import LabelEncoder
import pandas as pd
import sys
from scripts.config import COLUMN_MAPPING

def validate_input(df: pd.DataFrame):
    required_cols = list(COLUMN_MAPPING.values())
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in input data: {missing_cols}")

def preprocess_df(df: pd.DataFrame, label_encoders=None, training=True):
    """
    Preprocess the input DataFrame for training or inference.
    Args:
        df (pd.DataFrame): Input DataFrame containing flight data.
        label_encoders (optional dict): Dictionary of LabelEncoders for categorical features.
        training (bool): Flag indicating whether the function is used for training or inference.
    Returns:
        pd.DataFrame: Preprocessed DataFrame.
        dict: Dictionary of LabelEncoders if training is True, else the original label_encoders.
    """
    df = df.copy()

    # Validate input columns
    try:
        validate_input(df)
    except ValueError as e:
        sys.exit(f"Validation Error: {e}")

    # Keep only required columns (those in the mapping)
    df = df[list(COLUMN_MAPPING.values())]

    # Rename columns to expected names using mapping
    df.rename(columns={v: k for k, v in COLUMN_MAPPING.items()}, inplace=True)

    # Parse and engineer date features
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df["Month"] = df["Date"].dt.month
    df["Day"] = df["Date"].dt.day
    df["Weekday"] = df["Date"].dt.weekday

    df["Take-off Time (UTC)"] = pd.to_datetime(df["Take-off Time (UTC)"], errors='coerce')
    df["Takeoff_Hour"] = df["Take-off Time (UTC)"].dt.hour

    df.drop(columns=["Date", "Take-off Time (UTC)"], inplace=True, errors='ignore')

    # Encode categorical features
    categorical_cols = ["Aircraft", "Engine", "Origin Airport", "Destination Airport"]
    encoders = {} if training else (label_encoders or {})

    for col in categorical_cols:
        df[col] = df[col].astype(str)
        if training:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])
            encoders[col] = le
        else:
            le = encoders[col]
            df[col] = le.transform(df[col])

    return df, encoders
