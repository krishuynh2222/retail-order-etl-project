import pandas as pd

def extract_all_data():
    df_customers = pd.read_csv("data/rawData/df_Customers.csv")
    df_orders = pd.read_csv("data/rawData/df_Orders.csv")
    df_order_items = pd.read_csv("data/rawData/df_OrderItems.csv")
    df_payments = pd.read_csv("data/rawData/df_Payments.csv")
    df_products = pd.read_csv("data/rawData/df_Products.csv")

    return {
        "customers": df_customers,
        "orders": df_orders,
        "orderitems": df_order_items,
        "payments": df_payments,
        "products": df_products
    }

if __name__ == "__main__":
    dfs = extract_all_data()
    for name, df in dfs.items():
        print(f"\n Preview of {name.upper()}:")
        print(df.head(5))
        print(df.info())
