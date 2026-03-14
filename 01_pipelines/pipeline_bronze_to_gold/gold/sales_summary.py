import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import pipelines as dp


@dp.materialized_view(
    name="03_gold.d_sales_summary",
    partition_cols=["order_date"],
    table_properties={"quality": "gold"}
)
def d_sales_summary():
    d_sales_agg = (
        spark.table("ws_dbxproject.02_silver.fact_orders")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_amount").cast("decimal(10, 2)").alias("total_revenue"),
            F.avg("total_amount").cast("decimal(10, 2)").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("restaurant_id").alias("unique_restaurants"),
            F.sum(F.when(F.col("order_type") == "dine_in", 1).otherwise(0)).alias("dine_in_orders"),
            F.sum(F.when(F.col("order_type") == "take_away", 1).otherwise(0)).alias("take_away_orders"),
            F.sum(F.when(F.col("order_type") == "delivery", 1).otherwise(0)).alias("delivery_orders")
        )
        .select(
            "order_date", "total_orders", "total_revenue", "avg_order_value", "unique_customers", "unique_restaurants", "dine_in_orders", "take_away_orders", "delivery_orders")
    )

    return d_sales_agg

