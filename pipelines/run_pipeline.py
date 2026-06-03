import subprocess
import sys

print("=" * 60)
print("Starting RetailPulse-AI Pipeline")
print("=" * 60)

steps = [
    ("Bronze Ingestion", "src/ingestion/bronze_ingestion.py"),
    ("Silver Transformation", "src/transformations/silver_transformation.py"),
    ("Gold Transformation", "src/transformations/gold_transformation.py"),
    ("Data Mart Build", "src/data_marts/build_data_marts.py")
]

for step_name, script in steps:
    print(f"\nRunning {step_name}...")

    result = subprocess.run(
        [sys.executable, script]
    )

    if result.returncode != 0:
        print(f"{step_name} failed.")
        exit(1)

    print(f"{step_name} completed successfully.")

print("\nRetailPulse-AI pipeline completed successfully.")