from sqlalchemy import text
from db_conn import get_engine
import logging
from typing import Dict, Optional

engine = get_engine()  # instanciate database connection
logger = logging.getLogger(__name__)
SOURCE_TABLE = "retail_data_clean"


def _log_step_rows(step_name: str, rowcount: int) -> None:
    rows = rowcount if rowcount is not None and rowcount >= 0 else "unknown"
    logger.info(f"Step={step_name} || inserted_rows={rows}")


def _safe_rowcount(rowcount: int) -> Optional[int]:
    if rowcount is None or rowcount < 0:
        return None
    return rowcount


def create_olpt_table() -> bool:

    logger.info("Initializing OLTP schema and tables")

    # Transaction commits only if all DDL succeeds; otherwise it rolls back. feature that comes with sqlalchemy
    with engine.begin() as conn:
        # Create the Operational Schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS oltp;"))

        # Define Normalized OLTP Tables

        # Customers Table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS oltp.customers (
                customerid INT PRIMARY KEY,
                country VARCHAR(100)
            );
        """))

        # Products Table
        # Added a CHECK constraint on unitprice as required by the project
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS oltp.products (
                stockcode VARCHAR(50) PRIMARY KEY,
                description TEXT,
                unitprice NUMERIC CHECK (unitprice >= 0)
            );
        """))

        # Orders Table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS oltp.orders (
                invoiceno VARCHAR(50) PRIMARY KEY,
                invoicedate TIMESTAMP,
                customerid INT REFERENCES oltp.customers(customerid)
            );
        """))

        # Order_Items Table
        # Added a CHECK constraint to ensure quantity is >= zero
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS oltp.order_items (
                transaction_id INT PRIMARY KEY,
                invoiceno VARCHAR(50) REFERENCES oltp.orders(invoiceno),
                stockcode VARCHAR(50) REFERENCES oltp.products(stockcode),
                quantity INT CHECK (quantity >= 0)
            );
        """))

        logger.info("OLTP tables created successfully")

        return True


def load_oltp_table() -> Dict[str, Optional[int]]:
    logger.info(
        f"Loading data from source table {SOURCE_TABLE} to OLTP tables")

    # Transaction commits only if all inserts succeed; otherwise it rolls back.
    with engine.begin() as conn:
        # Insert Data into Customers
        # Using DISTINCT to avoid duplicate customer records
        customers_result = conn.execute(text(f"""
            INSERT INTO oltp.customers (customerid, country)
            SELECT DISTINCT customerid, country
            FROM {SOURCE_TABLE}
            WHERE customerid IS NOT NULL
            ON CONFLICT (customerid) DO NOTHING;
        """))
        _log_step_rows("insert_customers", customers_result.rowcount)

        # inserting into products
        # Using DISTINCT ON to resolve PrimeMart's duplicate product descriptions issue
        # This grabs the most recent description/price for each unique stockcode
        products_result = conn.execute(text(f"""
            INSERT INTO oltp.products (stockcode, description, unitprice)
            SELECT DISTINCT ON (stockcode) stockcode, description, unitprice
            FROM {SOURCE_TABLE}
            WHERE stockcode IS NOT NULL
            ORDER BY stockcode, invoicedate DESC
            ON CONFLICT (stockcode) DO NOTHING;
        """))

        # log how many product rows were inserted in this step
        _log_step_rows("insert_products", products_result.rowcount)

        # Insert into Orders table
        orders_result = conn.execute(text(
            f"""
            INSERT INTO oltp.orders (invoiceno,invoicedate,customerid)
            SELECT DISTINCT invoiceno,invoicedate,customerid
            FROM {SOURCE_TABLE}
            ON CONFLICT (invoiceno) DO NOTHING;
            """
        ))
        _log_step_rows("insert_orders", orders_result.rowcount)

        # Insert into Order_items table
        order_items_result = conn.execute(text(
            f"""
            INSERT INTO oltp.order_items (transaction_id, invoiceno, stockcode, quantity )
            SELECT DISTINCT transaction_id, invoiceno, stockcode, quantity
            FROM {SOURCE_TABLE}
            ON CONFLICT (transaction_id) DO NOTHING;
            """
        ))
        _log_step_rows("insert_order_items", order_items_result.rowcount)

        logger.info("OLTP data load completed")

        return {
            "insert_customers": _safe_rowcount(customers_result.rowcount),
            "insert_products": _safe_rowcount(products_result.rowcount),
            "insert_orders": _safe_rowcount(orders_result.rowcount),
            "insert_order_items": _safe_rowcount(order_items_result.rowcount),
        }


def start_process() -> Dict[str, object]:
    try:
        schema_ready = create_olpt_table()
        load_summary: Dict[str, Optional[int]] = {}
        if schema_ready:
            load_summary = load_oltp_table()

        known_counts = [count for count in load_summary.values()
                        if count is not None]
        all_counts_known = len(known_counts) == len(load_summary)
        is_no_op = bool(load_summary) and all_counts_known and all(
            count == 0 for count in known_counts)

        status = "no_op" if is_no_op else "success"
        total_inserted = sum(known_counts) if known_counts else 0

        return {
            "status": status,
            "source_table": SOURCE_TABLE,
            "schema_ready": schema_ready,
            "total_inserted": total_inserted,
            "rows_inserted": load_summary,
        }
    except Exception as e:
        logger.error(f"Normalisation Failed: {str(e)}", exc_info=True)
        raise
    finally:
        engine.dispose()  # Clean up connection pool
