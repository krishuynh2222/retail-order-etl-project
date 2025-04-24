# Script Purpose: run_all_transforms.py
#================================================================================================================
# This script executes the full data transformation workflow for the retail order dataset.
#================================================================================================================
# Steps performed:
# 1. Loads raw CSV files from `data/rawData/`
# 2. Applies transformation and cleaning logic for:
#    - customers
#    - orders
#    - orderitems
#    - products
#    - payments
# 3. Saves the cleaned DataFrames into `data/processedData/` as CSV files.
#================================================================================================================
# Each transformation step is handled by dedicated functions imported from `etl/transform.py`.
#================================================================================================================
# This script prepares data for quality checks (`check_cleaned_data.py`) and loading into PostgreSQL (`load.py`).
#================================================================================================================

import os
import pandas as pd
from etl.transform import (
    transform_customers,
    transform_orders,
    transform_orderitems,
    transform_products,
    transform_payments
)
def main():
    print("🚀 Starting transformation for all tables...")
  
    # === Load raw data ===
    df_customers_raw = pd.read_csv("data/rawData/df_Customers.csv")
    df_orders_raw = pd.read_csv("data/rawData/df_Orders.csv")
    df_orderItems_raw = pd.read_csv("data/rawData/df_OrderItems.csv")
    df_products_raw = pd.read_csv("data/rawData/df_Products.csv")
    df_payments_raw = pd.read_csv("data/rawData/df_Payments.csv")
  
    # === Transform data ===
    df_customers_clean = transform_customers(df_customers_raw)
    df_orders_clean = transform_orders(df_orders_raw)
    df_orderItems_clean = transform_orderitems(df_orderItems_raw)
    df_products_clean = transform_products(df_products_raw)
    df_payments_clean = transform_payments(df_payments_raw)
  
    # === Save cleaned files ===
    output_dir = "data/processedData"
    os.makedirs(output_dir, exist_ok=True)

    # Save cleaned file
    df_customers_clean.to_csv(os.path.join(output_dir, "clean_customers.csv"), index=False)
    df_orders_clean.to_csv(os.path.join(output_dir, "clean_orders.csv"), index=False)
    df_orderItems_clean.to_csv(os.path.join(output_dir, "clean_orderItems.csv"), index=False)
    df_products_clean.to_csv(os.path.join(output_dir, "clean_products.csv"), index=False)
    df_payments_clean.to_csv(os.path.join(output_dir, "clean_payments.csv"), index=False)
    print("All tables transformed and saved successfully.")

if __name__ == "__main__":
    main()
