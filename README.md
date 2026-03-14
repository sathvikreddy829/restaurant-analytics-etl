# Restaurant Analytics ETL

## 📌 Overview
A data engineering pipeline built on Databricks that ingests, transforms, and aggregates restaurant data — including real-time orders, customer records, and menu items — into analysis-ready gold tables.

---

## 🏗️ Architecture
The pipeline follows a **medallion architecture** (Bronze → Silver → Gold) using Spark Declarative Pipelines and Delta tables stored in Databricks catalog volumes.

![image alt](https://github.com/sathvikreddy829/restaurant-analytics-etl/blob/c8518fe0ac0106548a7eeb1e319ceaed414418d1/project_architecture.png)

---

## 📂 Data Sources
- **Streaming**: Live order events manually simulated into the raw volume, representing what would arrive via Azure Event Hubs in production.  
- **Batch (CSV)**: Five static datasets uploaded manually to the `ws_dbxproject.00_raw.raw` volume — customers, restaurants, menu_items, and orders.

---

## 🥉 Bronze Layer
- Orders are ingested as Delta tables with minimal transformation.  
- The three dimension tables (**customers, restaurants, menu items**) are ingested directly into Silver, skipping Bronze since they require no streaming processing.

---

## 🥈 Silver Layer
- Streaming tables are created from Bronze using Spark Declarative Pipelines.  
- Each table applies **data quality checks**:
  - Type casting  
  - Null handling  
  - Deduplication  
  - Domain validation (e.g., valid order status, payment method, positive amounts)

**Key Outputs:**
- `fact_orders`  
- `fact_order_items`  
- `dim_customers`  
- `dim_restaurants`  
- `dim_menu_items`

---

## 🥇 Gold Layer
Analysis-ready aggregated tables:

- **`sales_summary`**  
  Daily aggregates of orders, revenue, order types (dine-in / takeaway / delivery), and unique customers per day.

- **`customer_360`**  
  Full customer profile combining:
  - Lifetime spend  
  - Average order value  
  - Total orders  
  - Loyalty tier (Bronze → Silver → Gold → Platinum)  
  - Favorite restaurant  
  - Favorite menu item  
  - Review stats  

---

## ⚙️ Tech Stack
- Databricks  
- Spark Declarative Pipelines  
- PySpark  
- Delta Lake  
- Databricks SQL  
