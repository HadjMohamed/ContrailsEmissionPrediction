import pandas as pd
import joblib

from scripts.config import COLUMN_MAPPING

def validate_input(df: pd.DataFrame):
    required_cols = list(COLUMN_MAPPING.values())
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in input data: {missing_cols}")

def preprocess_df(df: pd.DataFrame, label_encoders_path:str):
    """
    Preprocess the input DataFrame for inference.
    - Handles date feature extraction
    - Encodes categorical features
    - Filters invalid rows 
    Args:
        df (pd.DataFrame): Merged DataFrame to preprocess
        label_encoders_path (str): Path to the label encoders file
    Returns:
        df (pd.DataFrame): Cleaned and encoded dataframe
    """
    df = df.copy()

    # Validate input columns
    try:
        validate_input(df)
    except ValueError as e:
        print(f"Error: {e}")
        
    # Rename mapped columns
    df = df[list(COLUMN_MAPPING.values())]
    df.rename(columns={v: k for k, v in COLUMN_MAPPING.items()}, inplace=True)

    # Dates extraction
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df["Month"] = df["Date"].dt.month
    df["Day"] = df["Date"].dt.day
    df["Weekday"] = df["Date"].dt.weekday
    df["Take-off Time (UTC)"] = pd.to_datetime(df["Take-off Time (UTC)"], errors='coerce')
    df["Takeoff_Hour"] = df["Take-off Time (UTC)"].dt.hour
    
    # Cleaning
    df.drop(columns=["Date", "Take-off Time (UTC)"], inplace=True, errors='ignore')
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Encoding
    categorical_cols = ["Aircraft", "Engine", "Origin Airport", "Destination Airport"]
    label_encoders = joblib.load(label_encoders_path)  
    encoders = (label_encoders or {})

    for col in categorical_cols:
        df[col] = df[col].astype(str)
        le = encoders.get(col)
        if le is None:
            raise ValueError(f"Missing LabelEncoder for column: {col}")

        # Boolean mask 
        known_labels = set(le.classes_)
        mask_known = df[col].isin(known_labels)
        df = df[mask_known]

        df[col] = le.transform(df[col])

    return df
