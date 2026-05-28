from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

# ----------------------------------------
# Spark Session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("RetailPulse Silver Transformation") \
    .getOrCreate()

BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"

os.makedirs(SILVER_BASE, exist_ok=True)


# ----------------------------------------
# Sales Transformation
# ----------------------------------------
print("Transforming sales data...")

sales_df = spark.read.parquet(f"{BRONZE_BASE}/sales")

sales_clean = sales_df.dropDuplicates(["transaction_id"]) \
    .filter(col("quantity") > 0) \
    .filter(col("price") > 0) \
    .withColumn(
        "validated_revenue",
        col("quantity") * col("price") * (1 - col("discount"))
    )

sales_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/sales")

print("Sales silver transformation complete")


# ----------------------------------------
# Inventory Transformation
# ----------------------------------------
print("Transforming inventory data...")

inventory_df = spark.read.parquet(f"{BRONZE_BASE}/inventory")

inventory_clean = inventory_df \
    .filter(col("stock_level") >= 0) \
    .withColumn(
        "inventory_status",
        when(
            col("stock_level") < col("reorder_point"),
            "LOW_STOCK"
        ).otherwise("SUFFICIENT")
    )

inventory_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/inventory")

print("Inventory silver transformation complete")


# ----------------------------------------
# Customer Events Transformation
# ----------------------------------------
print("Transforming customer events...")

events_df = spark.read.parquet(f"{BRONZE_BASE}/customer_events")

events_clean = events_df.dropDuplicates(["event_id"])

events_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/customer_events")

print("Customer events silver transformation complete")


# ----------------------------------------
# External Signals Transformation
# ----------------------------------------
print("Transforming external signals...")

signals_df = spark.read.parquet(f"{BRONZE_BASE}/external_signals")

signals_clean = signals_df.fillna({
    "holiday_flag": 0,
    "promotion_intensity": 0
})

signals_clean.write.mode("overwrite").parquet(f"{SILVER_BASE}/external_signals")

print("External signals silver transformation complete")


print("\nSilver transformation completed successfully.")

spark.stop()