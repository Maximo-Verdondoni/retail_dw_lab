# etl/transform/build_dim_warehouse.py
import pandas as pd

def build_dim_warehouse(data, output_path):
    """
    Genera una tabla de dimensión warehouse con campos:
    id, warehouse_name, city, province, country_code
    """
    dim_warehouse = data["warehouses"].copy()

    dim_warehouse = dim_warehouse.rename(columns={"warehouse_id": "id"})

    # Guardamos en warehouse/dim
    file_path = output_path / "dim" / "dim_warehouse.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dim_warehouse.to_csv(file_path, index=False)
    
    print(f"✅ dim_warehouse guardado en {file_path}")
    return dim_warehouse