from pyspark.sql import SparkSession
from pyspark.sql import Row

from quality_checks import (
    check_nulls,
    check_duplicates,
    check_negative_values,
    check_schema,
    check_row_count,
    check_revenue_threshold,
    check_transaction_count,
    check_inventory_status,
    check_customer_events,
    check_forecast_revenue
)

spark = SparkSession.builder \
    .appName("RetailPulse Quality Framework") \
    .getOrCreate()

results = []

# =====================================================
# SALES MART
# =====================================================

print("Checking Sales Mart...")

sales = spark.read.parquet(
    "data/marts/sales_mart"
)

results.append(
    check_row_count(sales)
)

results.append(
    check_schema(
        sales,
        [
            "region",
            "category",
            "total_revenue",
            "transaction_count"
        ]
    )
)

results.append(
    check_nulls(
        sales,
        "total_revenue"
    )
)

results.append(
    check_negative_values(
        sales,
        "total_revenue"
    )
)

results.append(
    check_revenue_threshold(
        sales,
        "total_revenue"
    )
)

results.append(
    check_transaction_count(
        sales,
        "transaction_count"
    )
)

results.append(
    check_duplicates(
        sales,
        ["region", "category"]
    )
)

# =====================================================
# INVENTORY MART
# =====================================================

print("Checking Inventory Mart...")

inventory = spark.read.parquet(
    "data/marts/inventory_mart"
)

results.append(
    check_row_count(inventory)
)

results.append(
    check_schema(
        inventory,
        [
            "inventory_status",
            "count"
        ]
    )
)

results.append(
    check_nulls(
        inventory,
        "inventory_status"
    )
)

results.append(
    check_inventory_status(
        inventory
    )
)

# =====================================================
# CUSTOMER MART
# =====================================================

print("Checking Customer Mart...")

customer = spark.read.parquet(
    "data/marts/customer_mart"
)

results.append(
    check_row_count(customer)
)

results.append(
    check_schema(
        customer,
        [
            "event_type",
            "count"
        ]
    )
)

results.append(
    check_nulls(
        customer,
        "event_type"
    )
)

results.append(
    check_customer_events(
        customer
    )
)

# =====================================================
# FORECAST MART
# =====================================================

print("Checking Forecast Mart...")

forecast = spark.read.parquet(
    "data/marts/forecast_mart"
)

results.append(
    check_row_count(
        forecast
    )
)

results.append(
    check_schema(
        forecast,
        [
            "region",
            "forecast_revenue"
        ]
    )
)

results.append(
    check_nulls(
        forecast,
        "forecast_revenue"
    )
)

results.append(
    check_forecast_revenue(
        forecast
    )
)


# =====================================================
# QUALITY REPORT
# =====================================================

normalized_results = []

for result in results:

    normalized_results.append(
        Row(
            check=result.get("check"),
            status=result.get("status"),
            count=int(result.get("count", 0))
        )
    )

report_df = spark.createDataFrame(
    normalized_results
)

report_df.show(
    truncate=False
)

print(
    "\nQuality report generated successfully."
)

spark.stop()