Restaurant Analytics ETL 

Overview:
A data engineering pipeline built on Databricks that ingests, transforms, and aggregates restaurant data — including real-time orders, customer records, and menu items — into analysis-ready gold tables.

Architecture:
The pipeline follows a medallion architecture (Raw → Bronze → Silver → Gold) using Spark Declarative Pipelines and Delta tables stored in Databricks catalog volumes.

Data Sources:
Streaming: Live order events manually simulated into the raw volume, representing what would arrive via Azure Event Hubs in production.\n
Batch (CSV): Five static datasets uploaded manually to the ws_dbxproject.00_raw.raw volume — customers, restaurants, menu_items and orders.

1.Bronze layer — Orders are ingested as Delta tables with minimal transformation. The three dimension tables (customers, restaurants, menu items) are ingested directly into Silver, skipping Bronze since they   
require no streaming processing.

2.Silver layer — Streaming tables are created from Bronze using Spark Declarative Pipelines. Each table applies data quality checks: type casting, null handling, deduplication , and domain validation (e.g. valid order status, payment method, positive amounts). 
Key outputs: fact_orders, fact_order_items, dim_customers, dim_restaurants, dim_menu_items.

3.Gold layer - Created analysis-ready aggregated tables.
d_sales_summary — Daily aggregates of orders, revenue, order types (dine-in / takeaway / delivery), and unique customers per day.
d_customer_360 — Full customer profile combining lifetime spend, average order value, total orders, loyalty tier (Bronze → Silver → Gold → Platinum), favorite restaurant, favorite menu item, and review stats.

Tech Stack:
Databricks
Spark Declarative Pipelines
PySpark
Delta Lake
Databricks SQL
