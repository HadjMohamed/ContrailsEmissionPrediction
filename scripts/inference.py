from scripts.preprocess import preprocess_df
from scripts.predictor import ContrailPredictor


import pandas as pd
import os
import sys
import numpy as np
from loguru import logger
from xgboost import XGBRegressor
    
def run_inference(input_path: str, output_path: str):
    """
    Run inference on the merged input data using the trained model and save the results.
    Args:
        input_path (str): Path to the merged input CSV file.
        output_path (str): Path to save the output CSV file with predictions.
    """
    try:
        merged_df = pd.read_csv(input_path)
    except FileNotFoundError:
        logger.error(f"Input file not found at : {input_path}")
        sys.exit(1)
        
    # Preprocessing & Prediction     
    df_predict=merged_df.copy() 
    predictor = ContrailPredictor()
    y_pred = predictor.predict_contrails(df_predict)
    merged_df["Predicted_Contrail_Impact"] = y_pred
    
    # Saving
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    merged_df.to_csv(output_path, index=False)
    logger.success(f"Predictions saved to {output_path}")
