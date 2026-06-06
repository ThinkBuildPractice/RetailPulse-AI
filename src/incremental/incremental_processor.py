from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================
# SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("RetailPulse Incremental Processor")
    .getOrCreate()
)

# ==========================================
# LOAD WATERMARK
# ==========================================

watermark_df = spark.read.csv(
    "data/watermark/pipeline_watermark",
    header=True,
    inferSchema=True
)

watermark_date = (
    watermark_df
    .filter(
        F.col("pipeline_name") == "retail_pipeline"
    )
    .select("last_processed_date")
    .collect()[0][0]
)

print(f"Last Watermark: {watermark_date}")

# ==========================================
# LOAD SOURCE DATA
# ==========================================

sales_df = spark.read.csv(
    "data/incremental/sales_transactions.csv",
    header=True,
    inferSchema=True
)

print("Source Data")

sales_df.show()

# ==========================================
# FILTER NEW RECORDS
# ==========================================

incremental_df = sales_df.filter(
    F.col("transaction_date") > F.lit(watermark_date)
)

print("Incremental Records")

incremental_df.show()

# ==========================================
# GET LATEST DATE
# ==========================================

latest_date = (
    incremental_df
    .agg(
        F.max("transaction_date")
        .alias("max_date")
    )
    .collect()[0]["max_date"]
)

latest_date = (
    incremental_df
    .agg(
        F.max("transaction_date").alias("max_date")
    )
    .collect()[0]["max_date"]
)

print(f"Latest Transaction Date: {latest_date}")

if latest_date is not None:

    new_watermark = spark.createDataFrame(
        [("retail_pipeline", latest_date)],
        ["pipeline_name", "last_processed_date"]
    )

    new_watermark.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("data/watermark/pipeline_watermark")

    print("Watermark Updated Successfully")

else:

    print("No New Records Found")
    print("Watermark Not Updated")

spark.stop()