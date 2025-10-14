# etl/transform/build_dim_inventory.py
import pandas as pd

def build_fact_inventory(data, output_path):
    """
    Genera una tabla de hechos inventory con campos:
    id, product_id, warehouse_id, quantity_on_hand, day_id, timestamp
    """
    fact_inventory = data["inventory"].copy()

    fact_inventory = fact_inventory.rename(columns={"inventory_id": "id"})

    # Cargamos dimension calendario
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    # Creamos diccionario fecha → calendar_id
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))
    
    #1- Separamos created_at en fecha y hora
    fact_inventory['last_updated_at'] = pd.to_datetime(fact_inventory['last_updated_at'])
    fact_inventory['last_updated_at_date'] = fact_inventory['last_updated_at'].dt.date
    fact_inventory['last_updated_at_time'] = fact_inventory['last_updated_at'].dt.time
    fact_inventory['last_updated_at_date_id'] = fact_inventory['last_updated_at_date'].map(calendar_map).astype("Int64")

    cols = [
        'id', 'product_id', 'warehouse_id', 'last_updated_at_date_id',
        'quantity_on_hand', 'last_updated_at_time'
    ]
    fact_inventory = fact_inventory[cols]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_inventory.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_inventory.to_csv(file_path, index=False)
    
    print(f"✅ fact_inventory guardado en {file_path}")
    return fact_inventory