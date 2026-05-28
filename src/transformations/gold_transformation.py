from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count
import os

spark = SparkSession.builder \
    .appName("RetailPulse Gold Transformation") \
    .getOrCreate()

SILVER_BASE = "data/silver"
GOLD_BASE = "data/gold"

os.makedirs(GOLD_BASE, exist_ok=True)

# ------------------------------------------
# SALES KPIs
# ------------------------------------------
print("Creating sales gold layer...")

sales_df = spark.read.parquet(f"{SILVER_BASE}/sales")

sales_gold = sales_df.groupBy(
    "region",
    "category"
).agg(
    sum("validated_revenue").alias("total_revenue"),
    count("transaction_id").alias("transaction_count")
)

sales_gold.write.mode("overwrite").parquet(
    f"{GOLD_BASE}/sales_kpis"
)

print("Sales gold complete")


# ------------------------------------------
# INVENTORY KPIs
# ------------------------------------------
print("Creating inventory gold layer...")

inventory_df = spark.read.parquet(
    f"{SILVER_BASE}/inventory"
)

inventory_gold = inventory_df.groupBy(
    "inventory_status"
).count()

inventory_gold.write.mode("overwrite").parquet(
    f"{GOLD_BASE}/inventory_kpis"
)

print("Inventory gold complete")


# ------------------------------------------
# CUSTOMER KPIs
# ------------------------------------------
print("Creating customer gold layer...")

events_df = spark.read.parquet(
    f"{SILVER_BASE}/customer_events"
)

customer_gold = events_df.groupBy(
    "event_type"
).count()

customer_gold.write.mode("overwrite").parquet(
    f"{GOLD_BASE}/customer_kpis"
)

print("Customer gold complete")

print("\nGold transformation completed successfully.")

spark.stop()