# Orchestrated ETL Pipeline - Architecture & Improvements

This directory contains the Airflow-orchestrated ETL pipeline for the Retail Data Warehouse project. All DAG tasks have been refactored for production-grade reliability, observability, and integration with Apache Airflow's XCom system.

## Overview

The orchestrated pipeline consists of multiple DAG tasks that collectively:

1. Extract and stage raw data from source systems
2. Normalize data into an OLTP schema
3. Transform normalized data into an OLAP star schema for analytics
4. Execute management intelligence queries

All tasks are designed to be Airflow-compatible with structured return values for cross-task communication via XCom.

---

## DAG Tasks

### 1. **normalisation.py** - OLTP Schema Creation & Data Loading

**Purpose:** Creates normalized database schema and loads cleaned data from staging table into OLTP tables.

**Entry Point:** `start_process() -> Dict[str, object]`

#### Key Improvements Implemented

##### ✅ Transaction Safety (Engine.begin())

- **Before:** Used `engine.connect()` + manual `conn.commit()`
- **After:** Uses `engine.begin()` context manager
- **Benefit:** Automatic rollback on exception, guarantees atomic writes

```python
# Transaction commits only if all DDL/DML succeeds; otherwise rolls back automatically
with engine.begin() as conn:
    conn.execute(text("CREATE TABLE ..."))
    conn.execute(text("INSERT INTO ..."))
    # No manual commit() needed
```

##### ✅ Fully Qualified Source Table References

- **Before:** `SELECT * FROM retail_data_clean` (relies on search_path)
- **After:** Centralized constant with fully qualified name
- **Benefit:** Explicit, environment-independent, easily configurable

```python
SOURCE_TABLE = "staging.retail_data_clean"

# Used in all queries via f-string interpolation
SELECT * FROM {SOURCE_TABLE} WHERE ...
```

##### ✅ Structured Per-Step Row Counting

- **Before:** No row count visibility
- **After:** Every INSERT captures rowcount and logs it
- **Benefit:** Audit trail, data quality validation, XCom integration

```python
customers_result = conn.execute(text(f"INSERT INTO oltp.customers ... FROM {SOURCE_TABLE}"))
_log_step_rows("insert_customers", customers_result.rowcount)
# Logs: "Step=insert_customers || inserted_rows=5234"
```

Helper function handles edge cases:

```python
def _safe_rowcount(rowcount: int) -> Optional[int]:
    if rowcount is None or rowcount < 0:
        return None
    return rowcount
```

##### ✅ Structured Logging (Logger instead of Print)

- **Before:** Mixed `print()` and `logger`
- **After:** Consistent `logger.info()` for all events
- **Benefit:** Structured logs parseable by Airflow, searchable in production

```python
logger.info("Initializing OLTP schema and tables")
logger.info("Step=insert_products || inserted_rows=3980")
logger.info("OLTP data load completed")
```

##### ✅ DAG-Friendly Return Payload with No-Op Detection

- **Before:** Function returned boolean
- **After:** Returns XCom-compatible dictionary with operation metadata
- **Benefit:** Downstream tasks can branch logic, audit results, track data volume

```python
return {
    "status": "success" | "no_op",           # no_op when all inserts = 0
    "source_table": "staging.retail_data_clean",
    "schema_ready": True,
    "total_inserted": 431818,               # Sum of all inserts
    "rows_inserted": {
        "insert_customers": 4372,
        "insert_products": 3950,
        "insert_orders": 22190,
        "insert_order_items": 401604
    }
}
```

#### OLTP Schema

Tables created:

- **oltp.customers** - Deduplicated customer records with PK(customerid)
- **oltp.products** - Product catalog with CHECK(unitprice >= 0)
- **oltp.orders** - Orders with FK to customers, PK(invoiceno)
- **oltp.order_items** - Order line items with CHECK(quantity >= 0), PK(transaction_id)

---

### 2. **analytics.py** - OLAP Star Schema Creation & Analytics Queries

**Purpose:** Creates dimensional and fact tables for analytical reporting, executes management intelligence queries.

**Entry Point:** `start_process() -> Dict[str, object]`

#### Key Improvements Implemented

##### ✅ Type Hints for All Functions

- **Before:** No return type annotations
- **After:** Full typing with `Dict[str, int]`, `Optional[pd.DataFrame]`, etc.
- **Benefit:** IDE autocomplete, self-documenting contracts, type checking

```python
def create_dim_tables() -> Dict[str, int]:
def create_fact_tables() -> int:
def run_analytics() -> Dict[str, object]:
def start_process() -> Dict[str, object]:
```

##### ✅ Dimension Table Row Count Capture

- **Before:** Tables created silently, no verification
- **After:** Row count logged after each CREATE TABLE AS SELECT
- **Benefit:** Verify data loaded, audit trail for data volumes

```python
dim_customer_count = conn.execute(text("SELECT COUNT(*) FROM olap.dim_customer;")).scalar()
dim_counts["dim_customer"] = dim_customer_count
logger.info(f"Created olap.dim_customer with {dim_customer_count} rows")
```

##### ✅ Error-Safe Query Execution Helper

- **Before:** Queries logged raw DataFrames, no error recovery
- **After:** Wrapped in try/except with structured result handling
- **Benefit:** Single query failure doesn't crash pipeline, graceful degradation

```python
def _safe_query(query_name: str, query: str) -> Optional[pd.DataFrame]:
    """Execute query with error handling and logging."""
    try:
        result = pd.read_sql_query(query, engine)
        logger.info(f"Query '{query_name}' executed successfully. Rows returned: {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Query '{query_name}' failed: {str(e)}", exc_info=True)
        return None  # Fail gracefully
```

##### ✅ Structured Analytics Logging

- **Before:** Logged entire DataFrames as f-string objects
- **After:** Logs only row counts and query names
- **Benefit:** Clean, parseable logs; reduced token overhead; audit-friendly

```python
# BEFORE (Bad):
logger.info(f"Top Products:\n{pd.read_sql_query(...)}")  # Logs entire table

# AFTER (Good):
df_products = _safe_query("top_products_by_revenue", query_products)
results["top_products_by_revenue"] = len(df_products) if df_products is not None else 0
logger.info("Query 'top_products_by_revenue' executed successfully. Rows returned: 5")
```

##### ✅ DAG-Integrated Orchestration Function

- **Before:** Individual functions; no orchestration logic
- **After:** Central `start_process()` that calls all stages and aggregates results
- **Benefit:** Single task entrypoint; seamless XCom integration; centralized error handling

```python
def start_process() -> Dict[str, object]:
    """DAG task entrypoint. Returns structured result for XCom."""
    try:
        logger.info("Starting analytics pipeline")
        
        dim_metrics = create_dim_tables()       # Returns {dim_name: row_count}
        fact_count = create_fact_tables()       # Returns row_count
        query_metrics = run_analytics()         # Returns {query_name: row_count}
        
        total_dim_rows = sum(dim_metrics.values())
        logger.info(f"Analytics pipeline completed. Dimensions: {total_dim_rows} rows, Fact: {fact_count} rows")
        
        return {
            "status": "success",
            "dimension_tables": dim_metrics,
            "fact_table_rows": fact_count,
            "analytics_queries": query_metrics,
        }
    except Exception as e:
        logger.error(f"Analytics pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        engine.dispose()  # Clean up connection pool
```

Example return payload:
```python
{
    "status": "success",
    "dimension_tables": {
        "dim_customer": 5000,
        "dim_product": 3980,
        "dim_date": 365
    },
    "fact_table_rows": 401604,
    "analytics_queries": {
        "top_countries_by_revenue": 5,
        "top_products_by_revenue": 5,
        "monthly_revenue_trend": 24
    }
}
```

#### OLAP Schema

Tables created:
- **olap.dim_customer** - Customer dimension (customerid, country)
- **olap.dim_product** - Product dimension (stockcode, description, unitprice)
- **olap.dim_date** - Date dimension (date_id, year, month, quarter, day_of_week)
- **olap.fact_sales** - Central fact table (transaction_id, invoiceno, date_id, customerid, stockcode, quantity, revenue)

Analytics queries executed:
1. **top_countries_by_revenue** - Top 5 countries by total revenue and order count
2. **top_products_by_revenue** - Top 5 products by units sold and revenue
3. **monthly_revenue_trend** - Monthly revenue aggregation

---

## XCom Integration for Airflow

Both tasks return structured dictionaries that can be used in Airflow DAGs for downstream task communication.

### Example DAG Usage

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import task functions
from dags.normalisation import start_process as normalisation_start
from dags.analytics import start_process as analytics_start

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
}

with DAG('retail_etl_pipeline', default_args=default_args) as dag:
    
    # Task 1: Normalisation
    norm_task = PythonOperator(
        task_id='normalisation',
        python_callable=normalisation_start,
    )
    
    # Task 2: Analytics (depends on norm_task)
    analytics_task = PythonOperator(
        task_id='analytics',
        python_callable=analytics_start,
    )
    
    # Access XCom from previous tasks
    def log_summary(**context):
        norm_result = context['task_instance'].xcom_pull(task_ids='normalisation')
        analytics_result = context['task_instance'].xcom_pull(task_ids='analytics')
        
        print(f"Normalisation Status: {norm_result['status']}")
        print(f"Total Rows Inserted: {norm_result['total_inserted']}")
        print(f"Fact Table Rows: {analytics_result['fact_table_rows']}")
    
    summary_task = PythonOperator(
        task_id='log_summary',
        python_callable=log_summary,
    )
    
    norm_task >> analytics_task >> summary_task
```

---

## Log Output Examples

### Normalisation Task Logs

```
INFO - Initializing OLTP schema and tables
INFO - Step=insert_customers || inserted_rows=4372
INFO - Step=insert_products || inserted_rows=3950
INFO - Step=insert_orders || inserted_rows=22190
INFO - Step=insert_order_items || inserted_rows=401604
INFO - OLTP data load completed
```

### Analytics Task Logs

```
INFO - Starting analytics pipeline
INFO - Building the OLAP Star Schema. Creating dimension tables
INFO - Created olap.dim_customer with 5000 rows
INFO - Created olap.dim_product with 3980 rows
INFO - Created olap.dim_date with 365 rows
INFO - Building the OLAP Star Schema. Creating fact tables
INFO - Created olap.fact_sales with 401604 rows
INFO - Running management analytics queries
INFO - Query 'top_countries_by_revenue' executed successfully. Rows returned: 5
INFO - Query 'top_products_by_revenue' executed successfully. Rows returned: 5
INFO - Query 'monthly_revenue_trend' executed successfully. Rows returned: 24
INFO - Analytics pipeline completed. Dimensions: 9345 rows, Fact: 401604 rows
```

---

## Production Considerations

### Error Handling

- All DAG tasks include try/except blocks
- Exceptions are logged with full stack traces (`exc_info=True`)
- Tasks re-raise exceptions to signal failure to Airflow

### Resource Management

- All database connections are explicitly disposed in finally blocks: `engine.dispose()`
- Prevents connection pool leaks in long-running Airflow environments

### Monitoring & Alerting

- All key metrics (row counts, status, query execution) are logged
- XCom payloads enable downstream alerting based on data volumes
- Example: Alert if `total_inserted == 0` (no-op detected)

### Data Quality Validation

- NULL checks on critical keys (customerid)
- CHECK constraints on numeric fields (unitprice >= 0, quantity >= 0)
- ON CONFLICT DO NOTHING for idempotency on re-runs

---

## Quick Start

### Running Tasks Standalone

```bash
# Activate virtual environment
source /path/to/venv/bin/activate

# Run normalisation task
python -c "from dags.normalisation import start_process; import json; print(json.dumps(start_process(), indent=2))"

# Run analytics task
python -c "from dags.analytics import start_process; import json; print(json.dumps(start_process(), indent=2))"
```

### Running with Airflow

```bash
# Trigger DAG
airflow dags trigger retail_etl_pipeline

# Monitor logs
airflow tasks logs retail_etl_pipeline normalisation
airflow tasks logs retail_etl_pipeline analytics
```

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'db_conn'"

- Ensure PYTHONPATH includes the dags directory
- Run from the orchestrated_version directory

### "Column 'invoicedate' not found"

- This was a known bug in analytics.py (mixed with order_items schema)
- Fixed in the latest version (JOIN condition corrected)

## Documentation & References

- [Airflow XCom Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [SQLAlchemy Connection Pooling](https://docs.sqlalchemy.org/en/20/core/pooling.html)
- [PostgreSQL DISTINCT ON](https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT)
- [Star Schema Design Patterns](https://en.wikipedia.org/wiki/Star_schema)

---

