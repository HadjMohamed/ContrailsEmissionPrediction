import os
import sys
import joblib
import pandas as pd
from xgboost import XGBRegressor
from dotenv import load_dotenv

from contrails.config import BASE_MODEL_DIR, MODEL_FILE, ENCODER_FILE, FEATURE_FILE

if any([BASE_MODEL_DIR == "", MODEL_FILE == "", ENCODER_FILE == "", FEATURE_FILE == ""]):
    raise ValueError("Environment variables MODEL_DIR, MODEL_FILE, ENCODER_FILE and FEATURE_FILE must be set.")

class ContrailPredictor:
    """
    Encapsulates the model loading, preprocessing, and prediction logic for contrail impact.
    """

    def __init__(self):
        model_path = os.path.join(BASE_MODEL_DIR, MODEL_FILE)
        encoder_path = os.path.join(BASE_MODEL_DIR, ENCODER_FILE)
        feature_path = os.path.join(BASE_MODEL_DIR, FEATURE_FILE)
        self.model = XGBRegressor()
        self.model.load_model(model_path)
        self.encoders = joblib.load(encoder_path)
        self.features = joblib.load(feature_path)
        self.categorical_cols = ["Aircraft", "Engine", "Origin Airport", "Destination Airport"]

    def predict_contrails(self, df: pd.DataFrame) -> pd.Series:
        """
        Predict contrails emissions on preprocessed & encoded data.
        Args:
            df (Pandas DataFrame): Raw merged input flight data
        Returns:
            Pandas Series: Predicted contrail impact values
        """
        df=df[self.features] 
        return self.model.predict(df)
    
    def encode_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Encode categorical features using the fitted label encoders.
        Args:
            df (pd.DataFrame): DataFrame to encode
        Returns:
            pd.DataFrame: Encoded DataFrame
        """
        df = df.copy()
        for col in self.categorical_cols:
            le = self.encoders.get(col)
            if le is None:
                raise ValueError(f"Missing LabelEncoder for column: {col}")

            df[col] = df[col].astype(str)

            known_labels = set(le.classes_)
            mask_known = df[col].isin(known_labels)
            df = df[mask_known]

            df[col] = le.transform(df[col])
        return df
    
    def decode_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Decode encoded categorical features to their original labels for better readability.
        Args:
            df (pd.DataFrame): DataFrame to decode
        Returns:
            pd.DataFrame: Decoded DataFrame
        """        
        df = df.copy()
        for col in self.categorical_cols:
            le = self.encoders.get(col)
            if le is not None and col in df.columns:
                df[col] = le.inverse_transform(df[col])
        return df
