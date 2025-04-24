#  Script Purpose: main.py
#============================================================================
# This script orchestrates the full ETL pipeline for the retail order dataset:
# 1. Extract raw CSV files
# 2. Transform and clean each table
# 3. Save cleaned outputs to `data/processedData/`
# 4. Load cleaned data into a PostgreSQL database
#============================================================================
# It acts as the single entry point for running the complete data workflow.
#============================================================================

import os
import pandas as pd
from etl.extract import extract_all_data
from etl.transform import transform_all
from etl.load import load_table, connect_db

# Save cleaned DataFrames to CSV
def save_cleaned_data(dfs: dict, output_dir: str = "data/processedData"):
    os.makedirs(output_dir, exist_ok=True)
    for name, df in dfs.items():
        output_path = os.path.join(output_dir, f"clean_{name}.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved cleaned {name} to {output_path}")

# PostgreSQL table schemas (columns in order)
TABLE_SCHEMAS = {
    'customers': ['customer_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
    'products': ['product_id', 'product_category_name', 'product_weight_g',
                 'product_length_cm', 'product_height_cm', 'product_width_cm'],
    'orders': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
               'order_approved_at', 'order_delivered_timestamp', 'order_estimated_delivery_date'],
    'orderitems': ['order_id', 'product_id', 'seller_id', 'price', 'shipping_charges'],
    'payments': ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']
}

#  Main ETL controller
def main():
    print("Starting full ETL pipeline...")

    # Extract raw data
    raw_dfs = extract_all_data()

    # Transform all tables
    cleaned_dfs = transform_all(raw_dfs)

    # Save cleaned data to processedData/
    save_cleaned_data(cleaned_dfs)

    # Load into PostgreSQL
    try:
        conn = connect_db()
        cursor = conn.cursor()
    except Exception as e:
        print(f" Database connection failed: {e}")
        return

    # Truncate tables (in reverse dependency order)
    print("\n Truncating PostgreSQL tables...")
    cursor.execute("""
        TRUNCATE TABLE
            payments,
            orderitems,
            orders,
            products,
            customers
        CASCADE;
    """)
    conn.commit()
    print("Tables truncated.\n")

    for table, columns in TABLE_SCHEMAS.items():
        df = cleaned_dfs[table]

        # Convert datetime columns if table is 'orders'
        if table == 'orders':
            for col in ['order_purchase_timestamp', 'order_approved_at',
                        'order_delivered_timestamp', 'order_estimated_delivery_date']:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Load cleaned data to DB
        load_table(df, table, columns, cursor, conn)

    # Close connection
    cursor.close()
    conn.close()
    print("\n Full ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()
