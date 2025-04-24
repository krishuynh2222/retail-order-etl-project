# Script Purpose: transform.py
#==========================================================================================================
# This script cleans and transforms raw DataFrames to prepare them for loading into a PostgreSQL database.
#==========================================================================================================
# It includes:
# - Individual `transform_*()` functions for each table (customers, orders, etc.)
# - A `transform_all()` master function that processes all tables at once

# Key tasks include:
# - Normalizing text
# - Converting timestamps
# - Imputing missing values
# - Removing duplicates
# - Resetting index
#==========================================================================================================
# The cleaned DataFrames are then ready to be saved or loaded into a relational database for analysis.
#==========================================================================================================

import pandas as pd

# ------------Clean the Customers Table------------
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'customers' DataFrame:
    - Normalize text fields
    - UPPERCASE city and state
    - Drop duplicates
    - Reset index
    """
    df['customer_city'] = df['customer_city'].str.strip().str.upper()
    df['customer_state'] = df['customer_state'].str.strip().str.upper()

    df_cleaned = df.drop_duplicates().reset_index(drop=True)

    print("\n TRANSFORMED: CUSTOMERS TABLE")
    print(df_cleaned.head(5))
    return df_cleaned

# ------------Clean the Orders Table------------
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'orders' DataFrame:
    - Convert date columns to datetime
    - Drop rows with missing values
    - Drop duplicates
    - Reset index
    """
    datetime_cols = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_timestamp',
        'order_estimated_delivery_date'
    ]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    df_cleaned = df.dropna().drop_duplicates().reset_index(drop=True)

    print("\n TRANSFORMED: ORDERS TABLE")
    print(df_cleaned.head(5))
    return df_cleaned

# ------------Clean the OrderItems Table------------
def transform_orderitems(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'orderitems' DataFrame:
    - Drop rows with nulls
    - Drop duplicates
    - Reset index
    """
    df_cleaned = df.dropna().drop_duplicates().reset_index(drop=True)

    print("\n TRANSFORMED: ORDERITEMS TABLE")
    print(df_cleaned.head(5))
    return df_cleaned

# ------------Clean the Products Table------------
def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'products' DataFrame:
    - Replace underscores in category names
    - Lowercase and trim whitespace
    - Fill missing category names with 'unknown'
    - Fill in missing numerics with the median
    - Drop duplicates
    - Reset index
    """
    df['product_category_name'] = df['product_category_name'].str.replace('_', ' ').str.lower().str.strip()
    df['product_category_name'] = df['product_category_name'].fillna('unknown')

    numeric_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in numeric_cols:
        median = df[col].median()
        missing = df[col].isna().sum()
        df[col] = df[col].fillna(median)
        print(f" Filled {missing} missing values in '{col}' with median: {median}")

    before_dup = df.duplicated().sum()
    df_cleaned = df.drop_duplicates().reset_index(drop=True)
    after_dup = df_cleaned.duplicated().sum()

    print(f"\n DUPLICATES BEFORE: {before_dup} | AFTER: {after_dup}")
    print("\n NULLS AFTER CLEANING:")
    print(df_cleaned.isnull().sum())
    print("\n TRANSFORMED: PRODUCTS TABLE")
    print(df_cleaned.head(5))
    return df_cleaned

# ------------Clean the Payments Table------------
def transform_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'payments' DataFrame:
    - Replace underscores in payment_type
    - Drop nulls and duplicates
    - Reset index
    """
    df['payment_type'] = df['payment_type'].str.replace('_', ' ').str.strip().str.title()
    df_cleaned = df.dropna().drop_duplicates().reset_index(drop=True)

    print("\n TRANSFORMED: PAYMENTS TABLE")
    print(df_cleaned.head(5))
    return df_cleaned

# ------------Transform Function------------
def transform_all(raw_dfs: dict) -> dict:
    """
    Runs all transform functions and returns a dictionary of cleaned DataFrames.
    Expects a dict of raw DataFrames with keys:
    'customers', 'orders', 'orderitems', 'products', 'payments'
    """
    cleaned_dfs = {
        "customers": transform_customers(raw_dfs["customers"]),
        "orders": transform_orders(raw_dfs["orders"]),
        "orderitems": transform_orderitems(raw_dfs["orderitems"]),
        "products": transform_products(raw_dfs["products"]),
        "payments": transform_payments(raw_dfs["payments"])
    }
    return cleaned_dfs

if __name__ == "__main__":
    from extract import extract_all_data

    raw_dfs = extract_all_data()
    cleaned = transform_all(raw_dfs)

    for name, df in cleaned.items():
        print(f"\n {name.upper()}: {len(df)} rows after cleaning.")
