from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date

spark = SparkSession.builder \
    .appName("RetailPulse SCD Type 2") \
    .getOrCreate()

# ==========================================
# LOAD DIMENSION
# ==========================================

dimension_df = spark.read.csv(
    "data/scd/customer_dimension.csv",
    header=True,
    inferSchema=True
)

print("Current Dimension")

dimension_df.show()

# ==========================================
# LOAD CDC
# ==========================================

cdc_df = spark.read.csv(
    "data/cdc/customer_changes.csv",
    header=True,
    inferSchema=True
)

print("CDC Changes")

cdc_df.show()

# ==========================================
# FILTER UPDATES ONLY
# ==========================================

updates = cdc_df.filter(
    F.col("op_type") == "U"
)

print("Update Records")

updates.show()

# ==========================================
# EXPIRE OLD RECORDS
# ==========================================

today = str(date.today())

expired_records = (
    dimension_df.alias("dim")
    .join(
        updates.select("customer_id").alias("upd"),
        on="customer_id",
        how="inner"
    )
    .withColumn(
        "end_date",
        F.lit(today)
    )
    .withColumn(
        "is_current",
        F.lit("N")
    )
)

print("Expired Records")

expired_records.show()

# ==========================================
# KEEP UNCHANGED RECORDS
# ==========================================

unchanged_records = (
    dimension_df.alias("dim")
    .join(
        updates.select("customer_id").alias("upd"),
        on="customer_id",
        how="left_anti"
    )
)

# ==========================================
# NEW CURRENT RECORDS
# ==========================================

new_records = updates.select(
    "customer_id",
    "customer_name",
    "city"
).withColumn(
    "effective_date",
    F.lit(today)
).withColumn(
    "end_date",
    F.lit("9999-12-31")
).withColumn(
    "is_current",
    F.lit("Y")
)

print("New Records")

new_records.show()

# ==========================================
# FINAL DIMENSION
# ==========================================

final_dimension = (
    unchanged_records
    .unionByName(expired_records)
    .unionByName(new_records)
)

print("Final Dimension")

final_dimension.orderBy(
    "customer_id",
    "effective_date"
).show(
    truncate=False
)

# ==========================================
# SAVE
# ==========================================

final_dimension.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "data/scd/customer_dimension_updated"
    )

print(
    "SCD Type 2 Dimension Saved Successfully"
)

spark.stop()