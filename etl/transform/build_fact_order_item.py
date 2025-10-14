# etl/transform/build_fact_order_item.py
import pandas as pd

def build_fact_order_item(data, output_path):
    """
    Genera una tabla de hechos order_item con campos:
    id, product_id, customer_id, campaign_id, line_number, quantity, unit_price, discount_amount,tax_amount
    """
    fact_order_item = data["order_items"].copy()

    #Cargamos orders y obtenemos customer_id
    orders = data["orders"].copy()
    orders["order_id"] = orders["order_id"].astype("int64")

    fact_order_item = fact_order_item.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left"
    )

    # Cargamos order_marketing y obtenemos campaign_id
    order_marketing = data["order_marketing"].copy()
    fact_order_item = fact_order_item.merge(
        order_marketing[["order_id", "campaign_id"]],
        on="order_id",
        how="left"
    )

    # campaign_id a int (soporta NaN con Int64 si es necesario)
    fact_order_item["campaign_id"] = fact_order_item["campaign_id"].astype("Int64")

    #Creamos surrogate key interno
    fact_order_item["id"] = range(1, len(fact_order_item) + 1)

    # Reordenamos columnas
    cols = ["id", "product_id", "customer_id", "campaign_id",
            "line_number","quantity", "unit_price", "discount_amount", "tax_amount"]
    fact_order_item = fact_order_item[cols]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_order_item.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_order_item.to_csv(file_path, index=False)
    
    print(f"✅ fact_order_item guardado en {file_path}")
    return fact_order_item
