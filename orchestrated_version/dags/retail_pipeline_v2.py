from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from extract_and_stage import start_process
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    def create_staging_engine_config(conn_id: str = "dwh_postgres"):
        """ 
        Preflight task: validates the Airflow connection and       exposes connection metadata (conn_id/host/port/schema) for downstream logging and task wiring. 

        The SQLAlchemy engine is created later inside extract_and_stage.py when staging actually runs.

        """
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        hook.get_conn().close()
        print(
            f"Connection validated: conn_id={conn_id}, "
            f"host={conn.host}, port={conn.port}, schema={conn.schema}"
        )
        return {
            "conn_id": conn_id,
            "host": conn.host,
            "port": conn.port,
            "schema": conn.schema,
        }

    @task
    def extract_and_stage(connection_meta):
        print("Extracting data and loading to staging...")
        conn_id = connection_meta["conn_id"]
        print(
            f"Using connection {conn_id} -> "
            f"{connection_meta['host']}:{connection_meta['port']}/{connection_meta['schema']}"
        )
        meta = start_process(conn_id=conn_id)
        return meta

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
    connection_meta = create_staging_engine_config()
    step1 = extract_and_stage(connection_meta)
    step2 = clean_and_transform(step1)
    step3 = load_to_oltp(step2)
    step4 = build_olap_star_schema(step3)
