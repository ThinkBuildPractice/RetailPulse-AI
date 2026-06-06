import os

print("=" * 50)
print("RetailPulse Pipeline Started")
print("=" * 50)

# -----------------------------------
# Data Quality
# -----------------------------------

print("\nRunning Data Quality Checks...")
os.system(
    "python3 src/quality/data_quality_checks.py"
)

# -----------------------------------
# CDC
# -----------------------------------

print("\nRunning CDC Framework...")
os.system(
    "python3 src/cdc/cdc_processor.py"
)

# -----------------------------------
# SCD Type 2
# -----------------------------------

print("\nRunning SCD Type 2...")
os.system(
    "python3 src/scd/scd_type2_processor.py"
)

# -----------------------------------
# Incremental Processing
# -----------------------------------

print("\nRunning Incremental Processing...")
os.system(
    "python3 src/incremental/incremental_processor.py"
)

print("\nPipeline Completed Successfully")