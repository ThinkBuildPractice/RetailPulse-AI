from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("RetailPulse Data Marts") \
    .getOrCreate()

GOLD_BASE = "data/gold"
MART_BASE = "data/marts"

os.makedirs(MART_BASE, exist_ok=True)

# --------------------------------
# Sales Mart
# --------------------------------
print("Building Sales Mart...")

sales = spark.read.parquet(
    f"{GOLD_BASE}/sales_kpis"
)

sales.write.mode("overwrite").parquet(
    f"{MART_BASE}/sales_mart"
)

print("Sales Mart complete")


# --------------------------------
# Inventory Mart
# --------------------------------
print("Building Inventory Mart...")

inventory = spark.read.parquet(
    f"{GOLD_BASE}/inventory_kpis"
)

inventory.write.mode("overwrite").parquet(
    f"{MART_BASE}/inventory_mart"
)

print("Inventory Mart complete")


# --------------------------------
# Customer Mart
# --------------------------------
print("Building Customer Mart...")

customer = spark.read.parquet(
    f"{GOLD_BASE}/customer_kpis"
)

customer.write.mode("overwrite").parquet(
    f"{MART_BASE}/customer_mart"
)

print("Customer Mart complete")

print("\nAll data marts built successfully.")

spark.stop()