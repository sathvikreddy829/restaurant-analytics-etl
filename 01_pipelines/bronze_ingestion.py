from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(
    name="orders",
    table_properties={"quality": "bronze"}
)
def orders():
    orders_schema = StructType([
            StructField('order_id', StringType(), True),
            StructField('timestamp', StringType(), True),   
            StructField('restaurant_id', StringType(), True),
            StructField('customer_id', StringType(), True),
            StructField('order_type', StringType(), True),
            StructField('items', StringType(), True),
            StructField('total_amount', DoubleType(), True),
            StructField('payment_method', StringType(), True),
            StructField('order_status', StringType(), True)
        ])
    
    
    orders_df = (
            spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .schema(orders_schema)
            .csv("/Volumes/ws_dbxproject/00_landing/raw_volume/data/historical_orders.csv")
    )

    
    orders_df = orders_df.withColumnRenamed("timestamp", "order_timestamp")

    return orders_df