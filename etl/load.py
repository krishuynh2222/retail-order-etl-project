# Script Purpose: load.py
# =================================================================================
# This script loads cleaned data from CSV files into a PostgreSQL database.
# =================================================================================
# Main tasks:
# - Connect to PostgreSQL using credentials from .env
# - Truncate existing data from target tables
# - Validate foreign key dependencies before insertion
# - Insert each cleaned row from `data/processedData/` into its respective table
# - Handle insert errors gracefully and log skipped rows
# =================================================================================
# The script ensures that only valid and cleaned data is loaded into the database, ready for analytical queries and reporting.
# =================================================================================

import os
import time
import pandas as pd
import psycopg2
from dotenv import load_dotenv

#  Load environment variables 
load_dotenv()

#  CONNECT TO DATABASE
def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

#  LOAD A SINGLE TABLE
def load_table(df: pd.DataFrame, table_name: str, columns: list, cursor, conn):
    print(f"\n Preparing to load {table_name} ({len(df)} rows)...")

    #  Foreign key validation 
    if table_name in ['orderitems', 'payments']:
        cursor.execute("SELECT order_id FROM orders")
        valid_orders = set(row[0] for row in cursor.fetchall())
        original_len = len(df)
        df = df[df['order_id'].isin(valid_orders)]

        if table_name == 'orderitems':
            cursor.execute("SELECT product_id FROM products")
            valid_products = set(row[0] for row in cursor.fetchall())
            df = df[df['product_id'].isin(valid_products)]

        print(f" Filtered {original_len - len(df)} invalid rows from {table_name}")

    error_count = 0
    for index, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        values = [row[col] for col in columns]
        try:
            cursor.execute(sql, values)
        except Exception as e:
            error_count += 1
            print(f" Skipped row {index} in {table_name}: {e}")
            continue

    conn.commit()
    print(f" Loaded: {table_name} ({len(df) - error_count} inserted, {error_count} skipped)")


def main():
    print(" Starting PostgreSQL load...")
    start_time = time.time()

    try:
        conn = connect_db()
        cursor = conn.cursor()
    except Exception as e:
        print(f" Failed to connect to PostgreSQL: {e}")
        return

    # 🧹 Truncate tables in reverse FK dependency order
    print("🧹 Truncating all tables before loading...")
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
    print(" Tables truncated.\n")

    #  Path to cleaned CSVs
    base_path = os.path.join("data", "processedData")

    #  Define table schemas
    tables = {
        'customers': ['customer_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
        'products': ['product_id', 'product_category_name', 'product_weight_g',
                     'product_length_cm', 'product_height_cm', 'product_width_cm'],
        'orders': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                   'order_approved_at', 'order_delivered_timestamp', 'order_estimated_delivery_date'],
        'orderitems': ['order_id', 'product_id', 'seller_id', 'price', 'shipping_charges'],
        'payments': ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']
    }

    for table, cols in tables.items():
        file_path = os.path.join(base_path, f"clean_{table}.csv")
        if not os.path.exists(file_path):
            print(f" File not found: {file_path}")
            continue

        print(f"\n Loading {file_path}")
        df = pd.read_csv(file_path)

        # Convert the datetime for 'orders'
        if table == 'orders':
            for col in ['order_purchase_timestamp', 'order_approved_at',
                        'order_delivered_timestamp', 'order_estimated_delivery_date']:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        load_table(df, table, cols, cursor, conn)

    cursor.close()
    conn.close()
    print(f"\n All data loaded into PostgreSQL successfully in {round(time.time() - start_time, 2)} seconds.")

if __name__ == "__main__":
    main()
