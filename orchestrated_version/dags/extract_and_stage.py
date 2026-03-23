import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_DIR = Path(__file__).resolve().parent


def get_engine(conn_id: str = "dwh_postgres"):
    hook = PostgresHook(postgres_conn_id=conn_id)
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


def start_process(conn_id: str = "dwh_postgres") -> dict:
    print('Checking source data.......\n')
    data_dir = BASE_DIR / 'data'
    csv_path = data_dir / 'Online_Retail.csv'
    xlsx_path = data_dir / 'Online_Retail.xlsx'

    if not csv_path.exists():

        print('CSV Source data does not exists. coverting original...')

        data_dir.mkdir(parents=True, exist_ok=True)

        df = pd.read_excel(xlsx_path)
        df.to_csv(csv_path, index=False)

    else:
        print('CSV source data found. Reading file...')

    df = pd.read_csv(csv_path)
    print(df.info())

    # stage dataframe
    stage_data(df, conn_id=conn_id)

    return {
        "staging_table": "retail_data_staging",
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
    }


def stage_data(df: pd.DataFrame, table_name: str = 'retail_data_staging', conn_id: str = "dwh_postgres") -> None:

    # typecheck df. It has to be a dataframe
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Expected df to be a pandas DataFrame')

    # db_url = os.getenv('DWH_URL')
    # db_url = resolve_db_url(conn_id)
    # print('db_url', db_url)
    # if not db_url:
    #     raise ValueError('DWH_URL is not set in environment variables')

    # db engine
    engine = get_engine(conn_id)

    # truncate then append new data.
    with engine.begin() as conn:  # transaction
        table_exists = conn.execute(text(
            """
            SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
                AND table_name = 'retail_data_staging'
            );
            """
        )).scalar_one()
        print(table_exists)
        if table_exists:
            conn.execute(text(f'TRUNCATE TABLE public."{table_name}"'))
            df.to_sql(table_name, conn, if_exists="append",
                      index=False, schema="public")
        else:
            df.to_sql(table_name, conn, index=False, schema="public")


if __name__ == '__main__':
    start_process()
