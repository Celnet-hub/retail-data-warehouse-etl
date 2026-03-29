from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_engine(conn_id: str = "dwh_postgres"):
    hook = PostgresHook(postgres_conn_id=conn_id)
    
    # get connection details configured in the UI
    airflow_conn = hook.get_connection(conn_id)

    db_name = airflow_conn.schema
    if not db_name:
        raise ValueError(f"Connection '{conn_id}' has no database/schema set")

    db_url = URL.create(
        drivername="postgresql+psycopg2",
        username=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port,
        database=db_name,
    )
    return create_engine(db_url)
