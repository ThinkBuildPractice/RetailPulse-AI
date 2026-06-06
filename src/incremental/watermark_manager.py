from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("RetailPulse Watermark Manager")
    .getOrCreate()
)

watermark_df = spark.read.csv(
    "data/watermark/pipeline_watermark.csv",
    header=True,
    inferSchema=True
)

print("Current Watermark")

watermark_df.show(truncate=False)

spark.stop()