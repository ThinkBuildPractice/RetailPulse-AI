import subprocess
import sys
import logging
import os

# ==========================================
# Logging Setup
# ==========================================

os.makedirs(
    "logs",
    exist_ok=True
)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "RetailPulse"
)

# ==========================================
# Pipeline Steps
# ==========================================

steps = [
    (
        "Bronze Ingestion",
        "src/ingestion/bronze_ingestion.py"
    ),
    (
        "Silver Transformation",
        "src/transformations/silver_transformation.py"
    ),
    (
        "Gold Transformation",
        "src/transformations/gold_transformation.py"
    ),
    (
        "Data Mart Build",
        "src/data_marts/build_data_marts.py"
    ),
    (
        "Quality Framework",
        "src/quality/quality_runner.py"
    )
]

print("=" * 60)
print("Starting RetailPulse-AI Pipeline")
print("=" * 60)

logger.info("Pipeline Started")

for step_name, script in steps:

    print(f"\nRunning {step_name}...")
    logger.info(f"Starting {step_name}")

    result = subprocess.run(
        [sys.executable, script]
    )

    if result.returncode != 0:

        print(f"{step_name} failed.")
        logger.error(f"{step_name} failed")

        logging.shutdown()
        sys.exit(1)

    print(f"{step_name} completed successfully.")
    logger.info(f"{step_name} completed successfully")

print("\nRetailPulse-AI pipeline completed successfully.")

logger.info(
    "Pipeline completed successfully"
)

logging.shutdown()