from pyspark.sql import functions as F


# ==========================================
# Null Check
# ==========================================

def check_nulls(df, column_name):

    null_count = df.filter(
        F.col(column_name).isNull()
    ).count()

    return {
        "check": f"Null Check ({column_name})",
        "status": "PASS" if null_count == 0 else "FAIL",
        "count": null_count
    }


# ==========================================
# Duplicate Check
# ==========================================

def check_duplicates(df, columns):

    if isinstance(columns, str):
        columns = [columns]

    duplicate_count = (
        df.groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return {
        "check": f"Duplicate Check ({','.join(columns)})",
        "status": "PASS" if duplicate_count == 0 else "FAIL",
        "count": duplicate_count
    }


# ==========================================
# Negative Value Check
# ==========================================

def check_negative_values(df, column_name):

    negative_count = df.filter(
        F.col(column_name) < 0
    ).count()

    return {
        "check": f"Negative Check ({column_name})",
        "status": "PASS" if negative_count == 0 else "FAIL",
        "count": negative_count
    }


# ==========================================
# Schema Validation
# ==========================================

def check_schema(df, expected_columns):

    actual_columns = set(df.columns)
    expected_columns = set(expected_columns)

    missing_columns = expected_columns - actual_columns

    return {
        "check": "Schema Validation",
        "status": "PASS" if len(missing_columns) == 0 else "FAIL",
        "count": len(missing_columns)
    }


# ==========================================
# Row Count Validation
# ==========================================

def check_row_count(df):

    row_count = df.count()

    return {
        "check": "Row Count Validation",
        "status": "PASS" if row_count > 0 else "FAIL",
        "count": row_count
    }


# ==========================================
# Revenue Validation
# ==========================================

def check_revenue_threshold(df, column_name):

    invalid_count = df.filter(
        F.col(column_name) <= 0
    ).count()

    return {
        "check": f"Revenue Threshold ({column_name})",
        "status": "PASS" if invalid_count == 0 else "FAIL",
        "count": invalid_count
    }


# ==========================================
# Transaction Validation
# ==========================================

def check_transaction_count(df, column_name):

    invalid_count = df.filter(
        F.col(column_name) <= 0
    ).count()

    return {
        "check": f"Transaction Validation ({column_name})",
        "status": "PASS" if invalid_count == 0 else "FAIL",
        "count": invalid_count
    }


# ==========================================
# Inventory Validation
# ==========================================

def check_inventory_status(df):

    allowed_values = [
        "LOW_STOCK",
        "SUFFICIENT"
    ]

    invalid_count = df.filter(
        ~F.col("inventory_status").isin(
            allowed_values
        )
    ).count()

    return {
        "check": "Inventory Status Validation",
        "status": "PASS" if invalid_count == 0 else "FAIL",
        "count": invalid_count
    }


# ==========================================
# Customer Event Validation
# ==========================================

def check_customer_events(df):

    allowed_events = [
        "purchase",
        "view",
        "add_to_cart",
        "abandon_cart"
    ]

    invalid_count = df.filter(
        ~F.col("event_type").isin(
            allowed_events
        )
    ).count()

    return {
        "check": "Customer Event Validation",
        "status": "PASS" if invalid_count == 0 else "FAIL",
        "count": invalid_count
    }


# ==========================================
# Forecast Validation
# ==========================================

def check_forecast_revenue(df):

    invalid_count = df.filter(
        F.col("forecast_revenue") <= 0
    ).count()

    return {
        "check": "Forecast Revenue Validation",
        "status": "PASS" if invalid_count == 0 else "FAIL",
        "count": invalid_count
    }