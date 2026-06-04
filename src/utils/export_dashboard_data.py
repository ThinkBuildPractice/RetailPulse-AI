from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RetailPulse Dashboard Export")
    .getOrCreate()
)

print("Loading marts...")

sales = spark.read.parquet("data/marts/sales_mart")
inventory = spark.read.parquet("data/marts/inventory_mart")
customer = spark.read.parquet("data/marts/customer_mart")

print("Exporting dashboard datasets...")

sales.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv("data/dashboard_exports/sales_dashboard")

inventory.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv("data/dashboard_exports/inventory_dashboard")

customer.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv("data/dashboard_exports/customer_dashboard")

# ==========================================
# Forecast Dashboard
# ==========================================

forecast_dashboard = spark.read.parquet(
    "data/marts/forecast_mart"
)

forecast_dashboard.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "data/dashboard_exports/forecast_dashboard"
    )

print("Dashboard export completed.")