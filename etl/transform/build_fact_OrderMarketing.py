# etl/transform/build_fact_OrderMarketing.py
import pandas as pd

def build_fact_OrderMarketing(data, output_path):
    """
    Genera una tabla de hechos OrderMarketing con campos:
    id,campaign_id,customer_id,product_id, utm_source,utm_medium,utm_campaign,utm_content,utm_term
    """
    fact_order_marketing = data["order_marketing"].copy()

    # Cargamos campaign y channel
    dim_campaign = pd.read_csv(output_path / "dim" / "dim_campaign.csv")


    # Validamos campaign_id existente en la dimensión ---
    fact_order_marketing = fact_order_marketing[
        fact_order_marketing["campaign_id"].isin(dim_campaign["campaign_id"])
    ]

    #campaign_id a int (si hay NaN, se pasa float)
    fact_order_marketing["campaign_id"] = fact_order_marketing["campaign_id"].astype("Int64")

    # Creamos ID interno (surrogate key)
    fact_order_marketing["id"] = range(1, len(fact_order_marketing) + 1)

    #---- Buscamos customer_id en order.csv
    orders = data["orders"].copy()

    # Nos aseguramos de que el id esté bien tipado
    orders["order_id"] = orders["order_id"].astype("int64")

    # Hacemos el join por order_id
    fact_order_marketing = fact_order_marketing.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left"
    )

    #---- Buscamos product_id en order_item.csv
    order_items = data["order_items"].copy()

    # Hacemos el join por order_id
    fact_order_marketing = fact_order_marketing.merge(
        order_items[["order_id", "product_id"]],
        on="order_id",
        how="left"
    )

    # Reordenamos columnas
    cols = ["id", "campaign_id", "customer_id", "product_id",
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]

    fact_order_marketing = fact_order_marketing[cols]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_OrderMarketing.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_order_marketing.to_csv(file_path, index=False)
    
    print(f"✅ fact_OrderMarketing guardado en {file_path}")
    return fact_order_marketing