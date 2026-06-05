from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
from pyspark.sql.functions import date_sub

# ==========================================
# SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("RetailPulse SCD Type 2")
    .getOrCreate()
)

today = F.current_date()

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
# LOAD CDC FILE
# ==========================================

cdc_df = spark.read.csv(
    "data/cdc/customer_changes.csv",
    header=True,
    inferSchema=True
)

print("CDC Changes")
cdc_df.show()

# ==========================================
# SPLIT CDC OPERATIONS
# ==========================================

inserts = cdc_df.filter(
    F.col("op_type") == "I"
)

updates = cdc_df.filter(
    F.col("op_type") == "U"
)

deletes = cdc_df.filter(
    F.col("op_type") == "D"
)

print("INSERTS")
inserts.show()

print("UPDATES")
updates.show()

print("DELETES")
deletes.show()

# ==========================================
# CURRENT / HISTORICAL RECORDS
# ==========================================

current_dim = dimension_df.filter(
    F.col("is_current") == "Y"
)

historical_dim = dimension_df.filter(
    F.col("is_current") == "N"
)

# ==========================================
# EXPIRE UPDATED RECORDS
# ==========================================

expired_update_records = (
    current_dim
    .join(
        updates.select("customer_id"),
        on="customer_id",
        how="inner"
    )
    .withColumn(
        "end_date",
        date_sub(today, 1)
    )
    .withColumn(
        "is_current",
        F.lit("N")
    )
)

print("Expired Update Records")
expired_update_records.show()

# ==========================================
# NEW CURRENT UPDATE RECORDS
# ==========================================

new_update_records = (
    updates.select(
        "customer_id",
        "customer_name",
        "city"
    )
    .withColumn(
        "effective_date",
        today
    )
    .withColumn(
        "end_date",
        F.lit("9999-12-31")
    )
    .withColumn(
        "is_current",
        F.lit("Y")
    )
)

print("New Update Records")
new_update_records.show()

# ==========================================
# PROCESS DELETES
# ==========================================

deleted_records = (
    current_dim
    .join(
        deletes.select("customer_id"),
        on="customer_id",
        how="inner"
    )
    .withColumn(
        "end_date",
        date_sub(today, 1)
    )
    .withColumn(
        "is_current",
        F.lit("N")
    )
)

print("Deleted Records")
deleted_records.show()

# ==========================================
# PROCESS INSERTS
# ==========================================

insert_records = (
    inserts
    .join(
        dimension_df.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    .select(
        "customer_id",
        "customer_name",
        "city"
    )
    .withColumn(
        "effective_date",
        today
    )
    .withColumn(
        "end_date",
        F.lit("9999-12-31")
    )
    .withColumn(
        "is_current",
        F.lit("Y")
    )
)

print("Insert Records")
insert_records.show()

# ==========================================
# KEEP UNCHANGED CURRENT RECORDS
# ==========================================

remaining_current = (
    current_dim
    .join(
        updates.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    .join(
        deletes.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
)

# ==========================================
# FINAL DIMENSION
# ==========================================

final_dimension = (
    historical_dim
    .unionByName(remaining_current)
    .unionByName(expired_update_records)
    .unionByName(new_update_records)
    .unionByName(deleted_records)
    .unionByName(insert_records)
)

print("Final Dimension")

final_dimension.orderBy(
    "customer_id",
    "effective_date"
).show(
    truncate=False
)

# ==========================================
# SAVE OUTPUT
# ==========================================

final_dimension.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "data/scd/customer_dimension_updated"
    )

print("SCD Type 2 Dimension Saved Successfully")

spark.stop()