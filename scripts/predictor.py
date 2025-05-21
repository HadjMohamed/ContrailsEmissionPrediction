import os
import sys
import joblib
import pandas as pd
from xgboost import XGBRegressor
from dotenv import load_dotenv

from scripts.config import BASE_MODEL_DIR, MODEL_FILE, ENCODER_FILE, FEATURE_FILE

if any([BASE_MODEL_DIR == "", MODEL_FILE == "", ENCODER_FILE == "", FEATURE_FILE == ""]):
    raise ValueError("Environment variables MODEL_DIR, MODEL_FILE, and ENCODER_FILE must be set.")

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

    def predict_contrails(self, df: pd.DataFrame) -> pd.Series:
        """
        Predict contrails emissions on preprocessed data.
        Args:
            df (Pandas DataFrame): Raw merged input flight data
        Returns:
            Pandas Series: Predicted contrail impact values
        """
        print("Columns in df:", df.columns.tolist())
        print("Expected features:", self.features)
        df=df[self.features] # Align training columns with input data
        return self.model.predict(df)
    
    def decode_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Decode encoded categorical features to their original labels for better readability.
        """
        categorical_cols = ["Aircraft", "Engine", "Origin Airport", "Destination Airport"]
        
        df = df.copy()
        for col in categorical_cols:
            le = self.encoders.get(col)
            if le is not None and col in df.columns:
                df[col] = le.inverse_transform(df[col])
        return df
