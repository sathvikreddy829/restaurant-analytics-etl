spark.sql("USE CATALOG `ws_dbxproject`")
spark.sql("USE SCHEMA `02_silver`")

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

##customers table 
@dp.table(
    name="customers",
    table_properties={"quality": "silver"}
)
def customers():
    customers_df = (
        spark.read.option("header", "true").csv("/Volumes/ws_dbxproject/00_landing/raw_volume/data/customers.csv")
    )
    return customers_df

##menu_items table
@dp.table(
    name="menu_items",
    table_properties={"quality": "silver"}
)
def menu_items():
    menu_items_df = (
        spark.read.option("header", "true").csv("/Volumes/ws_dbxproject/00_landing/raw_volume/data/menu_items.csv")
    )
    return menu_items_df


##restaurants table
@dp.table(
    name="restaurants",
    table_properties={"quality": "silver"}
)
def restaurants():
    restaurants_df = (
        spark.read.option("header", "true").csv("/Volumes/ws_dbxproject/00_landing/raw_volume/data/restaurants.csv")
    )
    return restaurants_df