# etl/transform/build_fact_shipment.py
import pandas as pd
def build_fact_shipment(data, output_path):
    """
    Genera una tabla de hechos shipment con campos:
    id,customer_id,campaign_id carrier,service_level,shipped_at_date_id,shipped_at_time,delivered_at_date_id,delivered_at_time,shipping_cost,tracking_number
    """
    fact_shipment = data["shipments"].copy()

    # Cargamos dimension calendario
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    # Creamos diccionario fecha → calendar_id
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))
    
    #1- Separamos shipped_at en fecha y hora
    fact_shipment['shipped_at'] = pd.to_datetime(fact_shipment['shipped_at'])
    fact_shipment['shipped_at_date'] = fact_shipment['shipped_at'].dt.date
    fact_shipment['shipped_at_time'] = fact_shipment['shipped_at'].dt.time
    fact_shipment['shipped_at_date_id'] = fact_shipment['shipped_at_date'].map(calendar_map).astype("Int64")

    #2- Separamos delivered_at en fecha y hora
    fact_shipment['delivered_at'] = pd.to_datetime(fact_shipment['delivered_at'])
    fact_shipment['delivered_at_date'] = fact_shipment['delivered_at'].dt.date
    fact_shipment['delivered_at_time'] = fact_shipment['delivered_at'].dt.time
    fact_shipment['delivered_at_date_id'] = fact_shipment['delivered_at_date'].map(calendar_map).astype("Int64")

    #--- Cargamos order.csv y traemos customer_id
    orders = data["orders"].copy()

    fact_shipment = fact_shipment.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left"
    )

    #--- Cargamos order_marketing y traemos campaign_id a traves de order_id
    order_marketing = data["order_marketing"].copy()

    fact_shipment = fact_shipment.merge(
        order_marketing[["order_id", "campaign_id"]],
        on="order_id",
        how="left"
    )
    #campaign_id a int (si hay NaN, se pasa float)
    fact_shipment["campaign_id"] = fact_shipment["campaign_id"].astype("Int64")

    # Creamos surrogate key interno
    fact_shipment["id"] = range(1, len(fact_shipment) + 1)

    # Reordenamos columnas
    cols = ["id", "customer_id", "campaign_id",
    "carrier", "service_level", "shipped_at_date_id", "shipped_at_time", "delivered_at_date_id", "delivered_at_time",
    "shipping_cost", "tracking_number"]
    fact_shipment = fact_shipment[cols]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_shipment.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_shipment.to_csv(file_path, index=False)
    
    print(f"✅ fact_shipment guardado en {file_path}")
    return fact_shipment