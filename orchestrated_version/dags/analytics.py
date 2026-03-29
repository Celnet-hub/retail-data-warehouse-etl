from sqlalchemy import text
import pandas as pd
import logging
from typing import Dict, Optional, List
from db_conn import get_engine


engine = get_engine()  # instanciate database connection
logger = logging.getLogger(__name__)


def create_dim_tables() -> Dict[str, int]:
    logger.info("Building the OLAP Star Schema. Creating dimension tables")

    dim_counts = {}
    with engine.begin() as conn:
        # Create the Analytics Schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS olap;"))

        # Create Dimension Tables

        # Dim_Customer
        conn.execute(text("DROP TABLE IF EXISTS olap.dim_customer CASCADE;"))
        conn.execute(text("""
            CREATE TABLE olap.dim_customer AS
            SELECT customerid, country
            FROM oltp.customers;
        """))
        conn.execute(
            text("ALTER TABLE olap.dim_customer ADD PRIMARY KEY (customerid);"))
        dim_customer_count = conn.execute(
            text("SELECT COUNT(*) FROM olap.dim_customer;")).scalar()
        dim_counts["dim_customer"] = dim_customer_count
        logger.info(
            f"Created olap.dim_customer with {dim_customer_count} rows")

        # Dim_Product
        conn.execute(text("DROP TABLE IF EXISTS olap.dim_product CASCADE;"))
        conn.execute(text("""
            CREATE TABLE olap.dim_product AS
            SELECT stockcode, description, unitprice
            FROM oltp.products;
        """))
        conn.execute(
            text("ALTER TABLE olap.dim_product ADD PRIMARY KEY (stockcode);"))
        dim_product_count = conn.execute(
            text("SELECT COUNT(*) FROM olap.dim_product;")).scalar()
        dim_counts["dim_product"] = dim_product_count
        logger.info(f"Created olap.dim_product with {dim_product_count} rows")

        # Dim_Date (A standard data warehouse practice to slice data by time periods)
        conn.execute(text("DROP TABLE IF EXISTS olap.dim_date CASCADE;"))
        conn.execute(text("""
            CREATE TABLE olap.dim_date AS
            SELECT DISTINCT 
                DATE(invoicedate) AS date_id,
                EXTRACT(YEAR FROM invoicedate) AS year,
                EXTRACT(MONTH FROM invoicedate) AS month,
                EXTRACT(QUARTER FROM invoicedate) AS quarter,
                TO_CHAR(invoicedate, 'Day') as day_of_week
            FROM oltp.orders;
        """))
        conn.execute(
            text("ALTER TABLE olap.dim_date ADD PRIMARY KEY (date_id);"))
        dim_date_count = conn.execute(
            text("SELECT COUNT(*) FROM olap.dim_date;")).scalar()
        dim_counts["dim_date"] = dim_date_count
        logger.info(f"Created olap.dim_date with {dim_date_count} rows")

    return dim_counts


def create_fact_tables() -> int:
    logger.info("Building the OLAP Star Schema. Creating fact tables")

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS olap.fact_sales CASCADE;"))

        conn.execute(text("""
            CREATE TABLE olap.fact_sales AS
            SELECT 
                oi.transaction_id,
                oi.invoiceno,
                DATE(o.invoicedate) AS date_id,
                o.customerid,
                oi.stockcode,
                oi.quantity,
                (oi.quantity * p.unitprice) AS revenue
            FROM oltp.order_items oi
            JOIN oltp.orders o ON oi.invoiceno = o.invoiceno
            JOIN oltp.products p ON oi.stockcode = p.stockcode;
        """))
        conn.execute(
            text("ALTER TABLE olap.fact_sales ADD PRIMARY KEY (transaction_id);"))
        fact_sales_count = conn.execute(
            text("SELECT COUNT(*) FROM olap.fact_sales;")).scalar()
        logger.info(f"Created olap.fact_sales with {fact_sales_count} rows")
        return fact_sales_count


def _safe_query(query_name: str, query: str) -> Optional[pd.DataFrame]:
    """Execute a query with error handling and logging. Returns DataFrame or None on failure."""
    try:
        result = pd.read_sql_query(query, engine)
        logger.info(
            f"Query '{query_name}' executed successfully. Rows returned: {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Query '{query_name}' failed: {str(e)}", exc_info=True)
        return None


def run_analytics() -> Dict[str, object]:
    """Execute analytics queries and return results as structured metrics."""
    logger.info("Running management analytics queries")
    results = {}

    # Calculate Total Revenue and Orders by Country
    query_country = """
        SELECT c.country, 
            ROUND(SUM(f.revenue), 2) AS total_revenue, 
            COUNT(DISTINCT f.invoiceno) AS total_orders
        FROM olap.fact_sales f
        JOIN olap.dim_customer c ON f.customerid = c.customerid
        GROUP BY c.country
        ORDER BY total_revenue DESC
        LIMIT 5;
    """
    df_country = _safe_query("top_countries_by_revenue", query_country)
    results["top_countries_by_revenue"] = len(
        df_country) if df_country is not None else 0

    # Top Products by Revenue
    query_products = """
        SELECT p.description, 
            SUM(f.quantity) AS total_units_sold, 
            ROUND(SUM(f.revenue), 2) AS total_revenue
        FROM olap.fact_sales f
        JOIN olap.dim_product p ON f.stockcode = p.stockcode
        GROUP BY p.description
        ORDER BY total_revenue DESC
        LIMIT 5;
    """
    df_products = _safe_query("top_products_by_revenue", query_products)
    results["top_products_by_revenue"] = len(
        df_products) if df_products is not None else 0

    # Monthly Sales Trend (Using our Date Dimension)
    query_trends = """
        SELECT d.year, d.month, 
            ROUND(SUM(f.revenue), 2) AS monthly_revenue
        FROM olap.fact_sales f
        JOIN olap.dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month;
    """
    df_trends = _safe_query("monthly_revenue_trend", query_trends)
    results["monthly_revenue_trend"] = len(
        df_trends) if df_trends is not None else 0

    return results


def start_process() -> Dict[str, object]:
    """DAG task entrypoint. Returns structured result for XCom."""
    try:
        logger.info("Starting analytics pipeline")

        # Create dimensions and capture metrics
        dim_metrics = create_dim_tables()

        # Create fact table and capture metrics
        fact_count = create_fact_tables()

        # Run management analytics queries
        query_metrics = run_analytics()

        total_dim_rows = sum(dim_metrics.values())
        logger.info(
            f"Analytics pipeline completed. Dimensions: {total_dim_rows} rows, Fact: {fact_count} rows")

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
