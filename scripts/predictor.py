import os
import sys
import joblib
import pandas as pd
from xgboost import XGBRegressor
from dotenv import load_dotenv
from loguru import logger
from scripts.preprocess import preprocess_df

# Load environment variables
load_dotenv()

MODEL_DIR = os.getenv("MODEL_DIR", "")
MODEL_FILE = os.getenv("MODEL_FILE", "")
ENCODER_FILE = os.getenv("ENCODER_FILE", "")

if any([MODEL_DIR == "", MODEL_FILE == "", ENCODER_FILE == ""]):
    logger.error("Environment variables MODEL_DIR, MODEL_FILE, and ENCODER_FILE must be set.")
    sys.exit(1)

class ContrailPredictor:
    """
    Encapsulates the model loading, preprocessing, and prediction logic for contrail impact.
    """

    def __init__(self):
        model_path = os.path.join(MODEL_DIR, MODEL_FILE)
        encoder_path = os.path.join(MODEL_DIR, ENCODER_FILE)
        self.model = XGBRegressor()
        self.model.load_model(model_path)
        self.encoders = joblib.load(encoder_path)

    def predict_contrails(self, df: pd.DataFrame) -> pd.Series:
        """
        Applies preprocessing and returns model predictions
        Args:
            df (Pandas DataFrame): Raw merged input flight data
        Returns:
            Pandas Series: Predicted contrail impact values
        """
        df_processed, _ = preprocess_df(df, training=False, label_encoders=self.encoders)
        return self.model.predict(df_processed)
