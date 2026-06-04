from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder \
    .appName("RetailPulse Data Marts") \
    .getOrCreate()

GOLD_BASE = "data/gold"
MART_BASE = "data/marts"

os.makedirs(MART_BASE, exist_ok=True)

# =====================================================
# SALES MART
# =====================================================

print("Building Sales Mart...")

sales = spark.read.parquet(
    f"{GOLD_BASE}/sales_kpis"
)

sales_mart = sales.select(
    "region",
    "category",
    "total_revenue",
    "transaction_count"
)

sales_mart.write.mode("overwrite").parquet(
    f"{MART_BASE}/sales_mart"
)

print(
    f"Sales Mart complete. Rows: {sales_mart.count()}"
)

# =====================================================
# INVENTORY MART
# =====================================================

print("Building Inventory Mart...")

inventory = spark.read.parquet(
    f"{GOLD_BASE}/inventory_kpis"
)

inventory_mart = inventory.select(
    "inventory_status",
    "count"
)

inventory_mart.write.mode("overwrite").parquet(
    f"{MART_BASE}/inventory_mart"
)

print(
    f"Inventory Mart complete. Rows: {inventory_mart.count()}"
)

# =====================================================
# CUSTOMER MART
# =====================================================

print("Building Customer Mart...")

customer = spark.read.parquet(
    f"{GOLD_BASE}/customer_kpis"
)

customer_mart = customer.select(
    "event_type",
    "count"
)

customer_mart.write.mode("overwrite").parquet(
    f"{MART_BASE}/customer_mart"
)

print(
    f"Customer Mart complete. Rows: {customer_mart.count()}"
)

# =====================================================
# FORECAST MART
# =====================================================

print("Building Forecast Mart...")

forecast_mart = sales.groupBy(
    "region"
).agg(
    F.sum("total_revenue").alias(
        "forecast_revenue"
    )
)

forecast_mart.write.mode("overwrite").parquet(
    f"{MART_BASE}/forecast_mart"
)

print(
    f"Forecast Mart complete. Rows: {forecast_mart.count()}"
)

# =====================================================
# SUMMARY
# =====================================================

print("\nAll data marts built successfully.")

spark.stop()