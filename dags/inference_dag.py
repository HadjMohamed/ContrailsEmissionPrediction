from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from pathlib import Path
#from google.cloud import bigquery
import pandas as pd
import os

# Local package import
from scripts.config import RAW_DATA_DIR, BASE_MODEL_DIR,ENCODER_FILE
from scripts.extraction.extract_airport import extract_airport, get_airports_last_modified
from scripts.extraction.extract_aircraft import extract_aircraft
from scripts.extraction.extract_flight import extract_flight
from scripts.merge_sources import merge_sources
from scripts.preprocess import preprocess_df
from scripts.inference import run_inference

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

with DAG(
    dag_id="merge_sources_pipeline",
    default_args=default_args,
    schedule_interval="@monthly",  
    catchup=False,
    tags=["contrail"]
) as dag:
    
    wait_for_flight_file = FileSensor(
    task_id="wait_for_flight_file",
    filepath=f"{RAW_DATA_DIR}/flight_{{{{ execution_date.strftime('%Y_%m') }}}}.csv",
    poke_interval=60 * 60 * 6, # 6 hours
    timeout=60 * 60 * 24 * 31, # 31 days
    mode="reschedule"  
    )
    
    wait_for_aircraft_file = FileSensor(
    task_id="wait_for_aircraft_file",
    filepath=f"{RAW_DATA_DIR}/aircraft_{{{{ execution_date.strftime('%Y_%m') }}}}.csv",
    poke_interval=60 * 60 * 6, # 6 hours
    timeout=60 * 60 * 24 * 31, # 31 days
    mode="reschedule"  
    )
    
    @task
    def wait_for_airport_update(**context):
        execution_date = context["execution_date"]
        last_mod = get_airports_last_modified()
        if execution_date.date() > last_mod.date():
            raise ValueError(f"Airports data too old: last modified at {last_mod}, expected at least {execution_date}")
        print(f"Airports last updated on: {last_mod}")
        
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
        label_encoders_path = os.path.join(BASE_MODEL_DIR, ENCODER_FILE)
        preprocessed_df = preprocess_df(merged_df, label_encoders_path)
        return preprocessed_df.to_dict(orient="records")
    
    @task
    def prediction(preprocessed_data):
        preprocessed_df = pd.DataFrame(preprocessed_data)
        result_df = run_inference(preprocessed_df)
        return result_df.to_dict(orient="records")
    
    # @task
    # def write_to_bigquery(prediction_result: list[dict]):
    #     """
    #     Inserts prediction results into a BigQuery table.

    #     Args:
    #         prediction_result (list[dict]): The list of prediction results to insert into BigQuery.
    #     """
    #     client = bigquery.Client()

    #     # Replace with your actual table ID
    #     table_id = "your-project-id.your_dataset.your_table"

    #     # Insert rows
    #     errors = client.insert_rows_json(table_id, prediction_result)

    #     if errors:
    #         raise RuntimeError(f"Failed to insert rows into BigQuery: {errors}")
    
    # Task dependencies

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

