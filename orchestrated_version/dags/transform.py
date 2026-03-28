import logging
from typing import Tuple
import pandas as pd
from db_conn import get_engine

logger = logging.getLogger(__name__)

# parameterize table names
STAGING_TABLE = "retail_data_staging"
REJECTED_TABLE = "retail_data_rejected"
CLEAN_TABLE = "retail_data_clean"
RETURNS_TABLE = "retail_data_returns"
SURROGATE_KEY_START = 5000


def transform_data() -> dict:
    """
    Transform staged retail data: clean invalid records, validate quantities,
    and separate returns from valid transactions.

    Returns:
        dict: Transformation statistics including row counts per table
    """
    engine = get_engine()  # Create fresh engine per task

    try:
        # Read staged data
        logger.info(f"Reading from {STAGING_TABLE}")
        staged_df = pd.read_sql_table(STAGING_TABLE, engine)

        if staged_df.empty:
            logger.warning("No data found in staging table")
            return {"status": "no_data", "rows_processed": 0}

        initial_count = len(staged_df)
        logger.info(f"Loaded {initial_count} records from staging")

        # Validate required columns exist
        required_columns = {'CustomerID',
                            'InvoiceDate', 'Quantity', 'UnitPrice'}
        missing_cols = required_columns - set(staged_df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # audit null CustomerID records
        null_customerid_df = staged_df[staged_df['CustomerID'].isnull()].copy()
        if not null_customerid_df.empty:
            null_customerid_df.to_sql(REJECTED_TABLE, engine,
                                      if_exists='append', index=False)
            logger.warning(
                f"Rejected {len(null_customerid_df)} records with null CustomerID")

        # Filter valid records
        valid_staged_df = staged_df[staged_df['CustomerID'].notna()].copy()
        valid_count = len(valid_staged_df)
        logger.info(f"Valid records for processing: {valid_count}")

        # Type conversions
        valid_staged_df['CustomerID'] = valid_staged_df['CustomerID'].astype(int)
        valid_staged_df['InvoiceDate'] = pd.to_datetime(valid_staged_df['InvoiceDate'])

        # Lowercase columns during transformation (not mid-pipeline)
        valid_staged_df.columns = valid_staged_df.columns.str.lower()

        # Create deterministic surrogate key using database sequence or max ID
        # Option: Use MAX(id) + row_number to ensure uniqueness on re-runs
        try:
            max_id = pd.read_sql_query(
                f"SELECT MAX(transaction_id) as max_id FROM {CLEAN_TABLE}",
                engine
            )['max_id'].iloc[0]
            start_id = (max_id or SURROGATE_KEY_START - 1) + 1
        except Exception:
            # Table doesn't exist yet
            start_id = SURROGATE_KEY_START

        valid_staged_df.insert(0, 'transaction_id',
                               range(start_id, start_id + len(valid_staged_df)))
        print(f"valid_staged_df : {valid_staged_df.head()}")
        # Separate returns - validate column names
        valid_df = valid_staged_df[
            (valid_staged_df['quantity'] > 0) &
            (valid_staged_df['unitprice'] > 0)
        ].copy()

        # getting a copy of records where quantity has negative value.
        returns_df = valid_staged_df[valid_staged_df['quantity'] < 0].copy() 

        logger.info(
            f"Split into {len(valid_df)} valid transactions, {len(returns_df)} sales returns")

        # Load transformed data
        stats = load_transformed_data(valid_df, returns_df, engine)
        logger.info(f"Transform complete: {stats}")

        return stats

    except Exception as e:
        logger.error(f"Transform failed: {str(e)}", exc_info=True)
        raise
    finally:
        engine.dispose()  # Clean up connection pool


def load_transformed_data(valid_df: pd.DataFrame,
                          returns_df: pd.DataFrame,
                          engine) -> dict:
    """
    Load transformed data to database tables.

    Args:
        valid_df: DataFrame with valid transactions
        returns_df: DataFrame with return transactions
        engine: SQLAlchemy engine

    Returns:
        dict: Statistics about loaded records
    """
    try:
        valid_df.to_sql(CLEAN_TABLE, engine,
                        if_exists='replace', index=False)
        valid_rows = len(valid_df)
        logger.info(f"Loaded {valid_rows} records to {CLEAN_TABLE}")

        returns_df.to_sql(RETURNS_TABLE, engine,
                          if_exists='replace', index=False)
        returns_rows = len(returns_df)
        logger.info(f"Loaded {returns_rows} records to {RETURNS_TABLE}")

        return {
            "status": "success",
            "valid_transactions": valid_rows,
            "returns": returns_rows,
            "total_processed": valid_rows + returns_rows
        }

    except Exception as e:
        logger.error(f"Load failed: {str(e)}", exc_info=True)
        raise
