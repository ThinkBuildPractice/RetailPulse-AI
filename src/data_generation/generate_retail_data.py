import os
import random
from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

fake = Faker()

# -------------------------------
# Configuration
# -------------------------------
NUM_TRANSACTIONS = 100000
NUM_PRODUCTS = 500
NUM_STORES = 50
NUM_CUSTOMERS = 10000

BASE_PATH = "data/raw"

os.makedirs(f"{BASE_PATH}/sales", exist_ok=True)
os.makedirs(f"{BASE_PATH}/inventory", exist_ok=True)
os.makedirs(f"{BASE_PATH}/customer_events", exist_ok=True)
os.makedirs(f"{BASE_PATH}/external_signals", exist_ok=True)

# -------------------------------
# Helper Data
# -------------------------------
regions = [
    "North",
    "South",
    "East",
    "West",
    "Central"
]

payment_types = [
    "Credit Card",
    "UPI",
    "Cash",
    "Wallet"
]

product_categories = [
    "Beverages",
    "Snacks",
    "Personal Care",
    "Household",
    "Dairy"
]

event_types = [
    "view",
    "add_to_cart",
    "purchase",
    "abandon_cart"
]

# -------------------------------
# Generate Sales Transactions
# -------------------------------
sales_data = []

for i in range(NUM_TRANSACTIONS):
    quantity = random.randint(1, 10)
    price = round(random.uniform(20, 500), 2)
    discount = round(random.uniform(0, 0.25), 2)

    sales_data.append({
        "transaction_id": f"T{i+1}",
        "store_id": random.randint(1, NUM_STORES),
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "product_id": random.randint(1, NUM_PRODUCTS),
        "category": random.choice(product_categories),
        "quantity": quantity,
        "price": price,
        "discount": discount,
        "revenue": round(quantity * price * (1 - discount), 2),
        "payment_type": random.choice(payment_types),
        "region": random.choice(regions),
        "timestamp": fake.date_time_between(
            start_date="-180d",
            end_date="now"
        )
    })

sales_df = pd.DataFrame(sales_data)

sales_df.to_csv(
    f"{BASE_PATH}/sales/sales_transactions.csv",
    index=False
)

print("Sales data generated")

# -------------------------------
# Generate Inventory Data
# -------------------------------
inventory_data = []

for product_id in range(1, NUM_PRODUCTS + 1):
    inventory_data.append({
        "product_id": product_id,
        "warehouse_id": random.randint(1, 10),
        "stock_level": random.randint(50, 5000),
        "reorder_point": random.randint(100, 500),
        "supplier_id": random.randint(1, 50),
        "last_restock_date": fake.date_between(
            start_date="-60d",
            end_date="today"
        )
    })

inventory_df = pd.DataFrame(inventory_data)

inventory_df.to_csv(
    f"{BASE_PATH}/inventory/inventory_snapshot.csv",
    index=False
)

print("Inventory data generated")

# -------------------------------
# Generate Customer Events
# -------------------------------
customer_events = []

for i in range(NUM_TRANSACTIONS):
    customer_events.append({
        "event_id": f"E{i+1}",
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "product_id": random.randint(1, NUM_PRODUCTS),
        "event_type": random.choice(event_types),
        "event_timestamp": fake.date_time_between(
            start_date="-180d",
            end_date="now"
        )
    })

events_df = pd.DataFrame(customer_events)

events_df.to_csv(
    f"{BASE_PATH}/customer_events/customer_events.csv",
    index=False
)

print("Customer event data generated")

# -------------------------------
# Generate External Signals
# -------------------------------
external_signals = []

for i in range(180):
    day = datetime.now() - timedelta(days=i)

    external_signals.append({
        "date": day.date(),
        "holiday_flag": random.choice([0, 1]),
        "temperature": round(random.uniform(18, 42), 1),
        "promotion_intensity": round(random.uniform(0, 1), 2)
    })

signals_df = pd.DataFrame(external_signals)

signals_df.to_csv(
    f"{BASE_PATH}/external_signals/external_signals.csv",
    index=False
)

print("External signals generated")

print("\nRetail enterprise raw data generation completed successfully.")