# Script Purpose: check_cleaned_data.py
#==================================================================================================================
# This script performs basic data quality checks on cleaned CSV files located in the `data/processedData/` folder).
#==================================================================================================================
# It verifies the following for each cleaned table (customers, orders, orderitems, products, payments):
# - File existence
# - Number of rows and columns
# - Missing (null) values per column
# - Duplicate rows
# - Sample preview of the first 5 rows
#==================================================================================================================
# This script can be run independently to ensure that transformed data is clean and ready for loading into a database.
#==================================================================================================================
# Usage:
#     python check_cleaned_data.py                # Uses default path: data/processedData/
#     python check_cleaned_data.py data/cleaned_no_duplicates  # Custom folder
#==================================================================================================================
# This helps validate ETL outputs before the `load.py` step in the pipeline.
#==================================================================================================================
import os
import pandas as pd
import sys

def check_cleaned_table(file_path: str, table_name: str):
    print(f"\n CHECKING: {table_name.upper()}")

    df = pd.read_csv(file_path)

    print(f" Shape: {df.shape}")
    print(f" Columns: {list(df.columns)}")

    missing = df.isnull().sum()
    print(f"\n Missing values: {missing[missing > 0] if missing.sum() > 0 else "None"}")

    duplicate_count = df.duplicated().sum()
    print(f"\n Duplicates: {duplicate_count if duplicate_count > 0 else 'None'}")

    print(f"\n Preview:\n{df.head(5)}")

def main():
    print(" Running data quality checks on cleaned CSVs...")

    base_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "processedData")

    files = {
        'customers': 'clean_customers.csv',
        'orders': 'clean_orders.csv',
        'orderitems': 'clean_orderitems.csv',
        'products': 'clean_products.csv',
        'payments': 'clean_payments.csv'
    }

    for table_name, file_name in files.items():
        path = os.path.join(base_path, file_name)
        if os.path.exists(path):
            check_cleaned_table(path, table_name)
        else:
            print(f" File not found: {file_name}")

    print("\n All checks complete.")

if __name__ == "__main__":
    main()
