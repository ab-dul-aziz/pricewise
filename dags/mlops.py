from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from datetime import timedelta

from utilization.scraping_link import ScrapingLink
from utilization.scraping_data import ScrapingData
from utilization.cleaning_data import CleaningData
from utilization.feature_engineering import FeatureEngineering
from utilization.fetch_from_postgresql import FetchFromPostgresql
from utilization.modeling import Modeling
from utilization.choose_best_model import ChooseBestModel

default_args= {
    'owner': 'GGGaming',
    'start_date': datetime(2024, 12, 16),
    'retry': None
}

with DAG(
    "house_prediction",
    description='End-to-end ML Pipeline House Prediction',
    schedule_interval='@monthly',
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1
    scraping_link = PythonOperator(
        task_id='scraping_link',
        python_callable=ScrapingLink
    )    

    # task: 2
    scraping_data = PythonOperator(
        task_id='scraping_data',
        python_callable=ScrapingData
    )

    # task: 3
    update_table_db = PostgresOperator(
        task_id="update_table_db",
        postgres_conn_id='postgres',
        sql='sql/update_table.sql'
    )
    
    # task: 4
    fetch_data = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=FetchFromPostgresql
    )

    # task: 5
    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=CleaningData
    )
        
    # task: 6
    feature_engineering = PythonOperator(
        task_id='feature_engineering',
        python_callable=FeatureEngineering
    )

    # task: 7
    modeling = PythonOperator(
        task_id='modeling',
        python_callable=Modeling,
        execution_timeout=timedelta(minutes=20)
    )

    # task: 8
    choose_best_model = PythonOperator(
        task_id='choose_best_model',
        python_callable=ChooseBestModel
    )

    scraping_link >> scraping_data >> update_table_db >> fetch_data >> cleaning_data >> feature_engineering >> modeling >> choose_best_model