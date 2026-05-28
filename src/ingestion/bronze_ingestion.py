from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os

spark = SparkSession.builder \
    .appName("RetailPulse Bronze Ingestion") \
    .getOrCreate()

RAW_BASE = "data/raw"
BRONZE_BASE = "data/bronze"

os.makedirs(BRONZE_BASE, exist_ok=True)


def ingest_to_bronze(source_path, target_path, source_name):
    print(f"Ingesting {source_name}...")

    df = spark.read.csv(
        source_path,
        header=True,
        inferSchema=True
    )

    df = df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    ).withColumn(
        "source_system",
        lit(source_name)
    )

    df.write.mode("overwrite").parquet(target_path)

    print(f"{source_name} ingested successfully")


ingest_to_bronze(
    f"{RAW_BASE}/sales/sales_transactions.csv",
    f"{BRONZE_BASE}/sales",
    "POS_SYSTEM"
)

ingest_to_bronze(
    f"{RAW_BASE}/inventory/inventory_snapshot.csv",
    f"{BRONZE_BASE}/inventory",
    "ERP_SYSTEM"
)

ingest_to_bronze(
    f"{RAW_BASE}/customer_events/customer_events.csv",
    f"{BRONZE_BASE}/customer_events",
    "DIGITAL_EVENTS"
)

ingest_to_bronze(
    f"{RAW_BASE}/external_signals/external_signals.csv",
    f"{BRONZE_BASE}/external_signals",
    "EXTERNAL_SIGNALS"
)

print("\nBronze ingestion completed successfully.")

spark.stop()