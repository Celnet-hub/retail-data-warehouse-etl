from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

# Airflow 2.x Default Arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'primemart_legacy_etl_v2',
    default_args=default_args,
    description='Airflow 2.x Legacy ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['airflow-2', 'legacy']
) as dag:

    @task
    def extract_and_stage():
        print("Extracting data and loading to staging...")
        # (Your pandas extraction code goes here)
        return "Staging Complete"

    @task
    def clean_and_transform(previous_step_status):
        print("Cleaning data with Pandas...")
        # (Your transformation code goes here)
        return "Cleaning Complete"

    @task
    def load_to_oltp(previous_step_status):
        print("Executing SQL to load data to OLTP partitioned tables...")
        # (Your SQLAlchemy OLTP code goes here)
        return "OLTP Load Complete"

    @task
    def build_olap_star_schema(previous_step_status):
        print("Executing SQL to build Fact and Dimension tables...")
        # (Your OLAP Star Schema code goes here)
        return "Pipeline Finished!"

    # Airflow 2 Dependency Chaining
    step1 = extract_and_stage()
    step2 = clean_and_transform(step1)
    step3 = load_to_oltp(step2)
    step4 = build_olap_star_schema(step3)
