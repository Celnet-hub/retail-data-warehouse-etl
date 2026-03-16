# Enterprise Retail Data Warehouse Optimization

## Project Overview

This project focuses on modernizing the data infrastructure for PrimeMart Retail Ltd, a multinational online retailer. The company's legacy system relied on flat-file Excel storage, leading to duplicate records, slow reporting queries, and a lack of centralized analytics. 

This repository contains the code to build an automated ETL pipeline that extracts raw data, normalizes it into a highly optimized OLTP database, and ultimately transforms it into an OLAP Star Schema for business intelligence.

## Tech Stack

* **Language:** Python
* **Libraries:** Pandas, SQLAlchemy, python-dotenv
* **Database:** PostgreSQL
* **Dataset:** [UCI Online Retail Dataset](!https://archive.ics.uci.edu/dataset/352/online+retail)

## Architecture & Workflow

### 1. Data Ingestion & Cleaning (Staging)

* Extracted raw data from Excel/CSV formats using Pandas.
* Isolated records with missing identifiers (`CustomerID`) into a separate audit table.
* Standardized data types and generated surrogate keys (`transaction_id`).

### 2. Database Initialization (OLTP Normalization)

* Created a structured `oltp` schema.
* Normalized the flat data into `Customers`, `Products`, `Orders`, and `Order_Items` tables.
* Enforced data integrity using Primary Keys, Foreign Keys, and CHECK constraints.
* Resolved legacy duplicate product descriptions using PostgreSQL's `DISTINCT ON` clause.

### 3. Performance Optimization

* Implemented single-column indexes on frequently queried columns (e.g., `invoicedate`, `customerid`).
* Built composite indexes to accelerate complex table joins.
* Applied declarative table partitioning (Range Partitioning by Month/Year) to the `Orders` and `Order_Items` tables to drastically reduce full table scans.

### 4. OLAP Transformation (Star Schema)

* Created an `olap` schema for analytical reporting.
* Generated Dimension tables (`dim_customer`, `dim_product`, `dim_date`) and a central Fact table (`fact_sales`).
* Pre-calculated metrics like total revenue to enable lightning-fast business intelligence queries.

## Key Analytics Queries Included

The project includes SQL scripts to generate actionable insights, such as:

* Top 5 Countries by Revenue
* Top 5 Products by Revenue
* Monthly Sales Trend Analysis

## How to Run

1. Clone this repository.
2. Ensure you have PostgreSQL installed and running.
3. Create a `.env` file in the root directory and add your database connection string: `DATABASE_URL=postgresql://username:password@localhost:5432/your_db_name`
4. Place the source dataset (`Online_Retail.xlsx`) in the `Source_Data` folder.
5. Execute the Python script to run the full ETL and database optimization pipeline.