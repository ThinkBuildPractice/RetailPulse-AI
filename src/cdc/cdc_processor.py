from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("RetailPulse CDC") \
    .getOrCreate()

# ==========================================
# LOAD CDC EVENTS
# ==========================================

cdc_df = spark.read.csv(
    "data/cdc/customer_changes.csv",
    header=True,
    inferSchema=True
)

print("Raw CDC Records")
cdc_df.show()

# ==========================================
# LOAD MASTER TABLE
# ==========================================

master_df = spark.read.csv(
    "data/cdc/customer_master.csv",
    header=True,
    inferSchema=True
)

print("Customer Master Records")
master_df.show()

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
# APPLY DELETES
# ==========================================

master_after_delete = master_df.join(
    deletes.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

print("After Delete")
master_after_delete.show()

# ==========================================
# APPLY UPDATES
# ==========================================

master_without_updates = master_after_delete.join(
    updates.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

updated_master = master_without_updates.unionByName(
    updates.select(
        "customer_id",
        "customer_name",
        "city"
    )
)

print("After Update")
updated_master.show()

# ==========================================
# APPLY INSERTS
# ==========================================

valid_inserts = inserts.join(
    deletes.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

new_records = valid_inserts.join(
    updated_master.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

final_master = updated_master.unionByName(
    new_records.select(
        "customer_id",
        "customer_name",
        "city"
    )
)

print("Final Customer Master")
final_master.show()

# ==========================================
# SAVE UPDATED MASTER
# ==========================================

final_master.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "data/cdc/customer_master_updated"
    )

print("Updated master saved successfully.")

spark.stop()