from datetime import datetime
from typing import List
from google.cloud import storage
from zipfile import ZipFile
import pandas as pd
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import dag, task  # DAG and task decorators for interfacing with the TaskFlow API

bucket_name = "corise-airflow-abhishek-week-1"
location = 'US'
storage_class = 'STANDARD'
client = GCSHook()
connection = client.get_conn()

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="@daily",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def energy_dataset_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using two simple tasks to extract data from a zipped folder
    and load it to GCS.

    """

    @task
    def bucket():

        client = GCSHook()
        connection = client.get_conn()

        try:
            connection.get_bucket(bucket_name)
            print('Bucket already exists.')
        except :
            client.create_bucket(bucket_name)

    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned.
        """
        # open zipped dataset
        with ZipFile("/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip")as data:
            csv_files = [file for file in data.namelist() if file.endswith('.csv')]
            for file in data.namelist():  
                if file.endswith(".csv"):
                    data.extract(file, "output_dir/")

        # Load each CSV file into a separate Pandas dataframe
        df=list()
        for  csv_file in csv_files:
            df.append(pd.read_csv(f'output_dir/{csv_file}'))
    
        return df 



    @task
    def load(unzip_result: List[pd.DataFrame],):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task, prints out the 
        schema, and then writes the data into GCS as parquet files.
        """
        # Create the GCS client
        client = GCSHook()   
        data_types = ['generation', 'weather']

        # Loop over the list and write each dataframe as a parquet file to GCS
        for i, dataframe in enumerate(unzip_result):
            unzip_result[i].to_parquet(f'{data_types[i]}.parquet')

            client.upload(
                        bucket_name=bucket_name,
                        object_name=f'{data_types[i]}.parquet',
                        filename=f'./{data_types[i]}.parquet')
        
    bucket()
    list_of_dataframes = extract()
    load(list_of_dataframes)
   
energy_dataset_dag = energy_dataset_dag()
