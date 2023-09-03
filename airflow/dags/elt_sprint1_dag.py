from src.extract import extract
from src import config
from src.load import load
from src.transform import run_queries
from sqlalchemy import create_engine
from pathlib import Path
from airflow import DAG
from datetime import datetime
from airflow.decorators import dag, task
import pickle


## ---------- LOAD ----------  
Path(config.SQLITE_BD_ABSOLUTE_PATH).touch()
ENGINE = create_engine(rf"sqlite:///{config.SQLITE_BD_ABSOLUTE_PATH}", echo=False)

## ---------- EXTRACT ----------  
csv_folder = config.DATASET_ROOT_PATH
public_holidays_url = config.PUBLIC_HOLIDAYS_URL
csv_table_mapping = config.get_csv_to_table_mapping()

## ---------- TRANSFORM ----------
RESULTS_PATH ="/opt/airflow/dags/results"

@dag(
    schedule=None,
    start_date=datetime(2023, 4, 17),
    catchup=False,
)

def etl_sprint1():
    """ELT pipeline using Airflow for Sprint_Project_1."""
    @task
    def extract_task():
        csv_folder = config.DATASET_ROOT_PATH
        public_holidays_url = config.PUBLIC_HOLIDAYS_URL
        csv_table_mapping = config.get_csv_to_table_mapping()
        dataframes=extract(csv_folder,csv_table_mapping,public_holidays_url)
        filename="intermediate_extract.pkl"
        with open(filename, "wb") as f:
            pickle.dump(dataframes, f)
        return filename
    

    @task
    def load_task(filename):
        with open(filename, "rb") as f:
            dataframes = pickle.load(f)
        load(dataframes,ENGINE)
        print('done load')
        return 2
    
    @task
    def transform_task(sql_load):
        queries=run_queries(ENGINE)      
        for name, df in queries.items():
            df.to_csv(f"{RESULTS_PATH}/{name}.csv")
        print('done all')
        return 3
        
    dataframes=extract_task()
    sql_load=load_task(dataframes)
    transform=transform_task(sql_load)


etl_sprint1()  




