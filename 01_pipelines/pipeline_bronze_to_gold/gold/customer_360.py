import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import pipelines as dp

@dp.materialized_view(
    name="ws_dbxproject.03_gold.d_customer_360",
    table_properties={"quality": "gold"}
)
def d_customer_360():
    df_orders = spark.table("ws_dbxproject.02_silver")
    df_restaurants = spark.table("ws_dbxproject.02_silver.restaurants")
    df_order_items = spark.table("ws_dbxproject.02_silver.fact_order_items")
    df_customers = spark.table("ws_dbxproject.02_silver.customers")

    df_order_stats = (
        df_orders
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_amount").alias("lifetime_spend"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.max("order_date").alias("last_order_date")
        )
    )

    df_fav_restaurant = (
        df_orders
        .groupBy("customer_id", "restaurant_id")
        .agg(F.countDistinct("order_id").alias("order_ct"))
        .withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("order_ct"))))
        .filter(F.col("rn") == 1)
        .join(df_restaurants, "restaurant_id", "left")
        .select(F.col("customer_id"), F.col("name").alias("favorite_restaurant"))
    )

    
    df_fav_item = (
        df_orders.join(df_order_items, "order_id", "inner")
        .groupBy(df_orders.customer_id , df_order_items.item_name)
        .agg(F.sum("quantity").alias("item_qty"))
        .withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("item_qty"))))
        .filter(F.col("rn") == 1)
        .select("customer_id", F.col("item_name").alias("favorite_item"))
    )

    
    df_c360 = (
        df_customers
        .join(df_order_stats, "customer_id", "left")
        .join(df_fav_restaurant, "customer_id", "left")
        .join(df_fav_item, "customer_id", "left")
        .select(
            F.col("customer_id"),
            F.col("name").alias("customer_name"),
            F.col("email"),
            df_customers.city,
            F.to_date(F.col("join_date")).alias("join_date"),
            F.coalesce(F.col("total_orders"), F.lit(0)).cast("bigint").alias("total_orders"),
            F.coalesce(F.col("avg_order_value"), F.lit(0)).cast("decimal(10, 2)").alias("avg_order_value"),
            F.coalesce(F.col("lifetime_spend"), F.lit(0)).cast("decimal(20, 2)").alias("lifetime_spend"),
            F.col("last_order_date"),
            F.when(F.col("lifetime_spend") >= 5000, "Platinum")
            .when(F.col("lifetime_spend") >= 2000, "Gold")
            .when(F.col("lifetime_spend") >= 500, "Silver")
            .otherwise("Bronze").alias("loyality_tier"),
            F.col("favorite_restaurant"),
            F.col("favorite_item"),
            F.when(
                F.col("lifetime_spend") >= 5000, 
                True
            ).otherwise(False).alias("is_vip")
        )
    )

    return df_c360