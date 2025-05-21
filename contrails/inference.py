import pandas as pd
from loguru import logger

from contrails.predictor import ContrailPredictor

def run_inference(preprocessed_df: pd.DataFrame)-> pd.DataFrame:
    """
    Run inference on the merged and preprocessed input data using the trained model and save the results.
    Args:
        input_path (str): Path to the merged input CSV file.
        output_path (str): Path to save the output CSV file with predictions.
    """
    df_predict=preprocessed_df.copy()
    predictor = ContrailPredictor()
    df_predict = predictor.encode_features(df_predict)
    y_pred = predictor.predict_contrails(df_predict)
    df_predict["Predicted_Contrail_Impact"] = y_pred
    logger.success(f"Predictions succeeded !")
    df_predict = predictor.decode_features(df_predict)

    return df_predict
