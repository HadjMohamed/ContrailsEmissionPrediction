from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.sensors.filesystem import FileSensor # type: ignore
from airflow.models import Variable # type: ignore
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import os

from contrails.config import RAW_DATA_DIR
from contrails.extraction.extract_airport import extract_airport, get_airports_last_modified
from contrails.extraction.extract_aircraft import extract_aircraft
from contrails.extraction.extract_flight import extract_flight
from contrails.merge_sources import merge_sources
from contrails.preprocess import preprocess_df
from contrails.inference import run_inference

"""
This DAG is designed to run the contrail prediction pipeline on a monthly basis on the first day of each month
For that, it waits for the files during 31 days in order to cover all the cases.
It will then extract the data from the files, merge them, preprocess them and run the inference.
"""

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

with DAG(
    dag_id="contrails_prediction_pipeline",
    default_args=default_args,
    schedule_interval="@monthly",  
    catchup=False,
    tags=["contrail"]
) as dag:
    
    wait_for_flight_file = FileSensor(
    task_id="wait_for_flight_file",
    filepath=f"raw_data/flight_{{{{ execution_date.strftime('%Y_%m') }}}}.csv",
    poke_interval=60 * 60 * 6, # 6 hours
    timeout=60 * 60 * 24 * 31, # 31 days
    mode="reschedule"  
    )
    
    wait_for_aircraft_file = FileSensor(
    task_id="wait_for_aircraft_file",
    filepath=f"raw_data/aircraft_{{{{ execution_date.strftime('%Y_%m') }}}}.csv",
    poke_interval=60 * 60 * 6, # 6 hours
    timeout=60 * 60 * 24 * 31, # 31 days
    mode="reschedule"  
    )
    
    @task(retries=124, retry_delay=timedelta(hours=6))    
    def wait_for_airport_update(**context):
        execution_date = context["execution_date"]
        last_mod = get_airports_last_modified()
        if execution_date.date() > last_mod.date():
            raise Exception(f"Airports data too old: last modified at {last_mod}, expected at least {execution_date}")
        logger.info(f"Airports last updated on: {last_mod}")
        
    @task
    def airport_extraction():
        airport_df = extract_airport()
        return airport_df.to_dict(orient="records")

    @task
    def aircraft_extraction(**context):
        month_str = context["execution_date"].strftime("%Y_%m")
        file_path = os.path.join(RAW_DATA_DIR,f"aircraft_{month_str}.csv")
        aircraft_df = extract_aircraft(file_path)
        return aircraft_df.to_dict(orient="records")
    
    @task
    def flight_extraction(**context):
        month_str = context["execution_date"].strftime("%Y_%m")
        file_path = os.path.join(RAW_DATA_DIR,f"flight_{month_str}.csv")
        flight_df = extract_flight(file_path)
        return flight_df.to_dict(orient="records")
    
    @task
    def run_merge(flight_data, aircraft_data, airport_data):
        flight_df = pd.DataFrame(flight_data)
        aircraft_df = pd.DataFrame(aircraft_data)
        airport_df = pd.DataFrame(airport_data)
        return merge_sources(flight_df, aircraft_df, airport_df).to_dict(orient="records")
    
    @task
    def preprocessing(merged_data):
        merged_df = pd.DataFrame(merged_data)
        preprocessed_df = preprocess_df(merged_df)
        return preprocessed_df.to_dict(orient="records")
    
    @task
    def prediction(preprocessed_data):
        preprocessed_df = pd.DataFrame(preprocessed_data)
        result_df = run_inference(preprocessed_df)
        return result_df.to_dict(orient="records")
    
    @task
    def save_results_to_csv(df_result, **context):
        month_str = context["execution_date"].strftime("%Y_%m")
        output_path = Variable.get("INFERENCE_OUTPUT_CSV", default_var=f"/home/airflow/gcs/data/output/inference_results_{month_str}.csv")
        df = pd.DataFrame(df_result)
        df.to_csv(output_path, index=False)
        logger.info(f"Results saved in {output_path}")

    # Initialize tasks
    flights = flight_extraction()
    aircrafts = aircraft_extraction()
    airports = airport_extraction()
    
    # Waiting
    wait_for_flight_file >> flights  # type: ignore
    wait_for_aircraft_file >> aircrafts  # type: ignore
    wait_for_airport_update() >> airports  # type: ignore

    # Inference pipeline
    merged_df = run_merge(flights, aircrafts, airports)
    preprocessed = preprocessing(merged_df)
    prediction_result = prediction(preprocessed)
    save_results_to_csv(prediction_result)

