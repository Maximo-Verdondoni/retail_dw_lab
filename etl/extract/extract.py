# etl/extract.py
import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw")

def load_csv(filename: str) -> pd.DataFrame:
    """
    Carga un CSV desde data/raw/ y lo devuelve como DataFrame.
    """
    file_path = RAW_PATH / filename
    return pd.read_csv(file_path)

def extract_all():
    """
    Carga todas las tablas raw y devuelve un diccionario {nombre: DataFrame}.
    """
    data = {
        "categories": load_csv("categories.csv"),
        "products": load_csv("products.csv"),
        "warehouses": load_csv("warehouses.csv"),
        "inventory": load_csv("inventory.csv"),
        "customers": load_csv("customers.csv"),
        "customer_addresses": load_csv("customer_addresses.csv"),
        "channels": load_csv("channels.csv"),
        "campaigns": load_csv("campaigns.csv"),
        "orders": load_csv("orders.csv"),
        "order_items": load_csv("order_items.csv"),
        "payments": load_csv("payments.csv"),
        "shipments": load_csv("shipments.csv"),
        "order_marketing": load_csv("order_marketing.csv"),
    }
    return data

if __name__ == "__main__":
    dfs = extract_all()
    for name, df in dfs.items():
        print(f"{name}: {df.shape[0]} filas, {df.shape[1]} columnas")
