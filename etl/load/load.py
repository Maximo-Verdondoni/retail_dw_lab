# etl/load.py
from pathlib import Path
from etl.extract.extract import extract_all
from etl.transform.build_dim_product import build as build_dim_product
from etl.transform.build_dim_calendar import build_dim_calendar
from etl.transform.build_dim_customer import build_dim_customer
from etl.transform.build_dim_warehouse import build_dim_warehouse
from etl.transform.build_dim_campaign import build_dim_campaign
from etl.transform.build_fact_inventory import build_fact_inventory
from etl.transform.build_fact_OrderMarketing import build_fact_OrderMarketing
from etl.transform.build_fact_shipment import build_fact_shipment
from etl.transform.build_fact_payment import build_fact_payment
from etl.transform.build_fact_order_item import build_fact_order_item
#from etl.transform import build_fact_orders as fact_orders


OUTPUT_PATH = Path("warehouse")  #A donde apunta el pipeline

def run_pipeline():
    data = extract_all()

    print("Construyendo dimensiones y hechos...")

    df_dim_products = build_dim_product(data, OUTPUT_PATH)
    df_dim_calendar = build_dim_calendar(OUTPUT_PATH, "2025-01-01", "2025-12-31")
    df_dim_customer = build_dim_customer(data, OUTPUT_PATH)
    df_dim_warehouse = build_dim_warehouse(data, OUTPUT_PATH)
    df_dim_campaign = build_dim_campaign(data, OUTPUT_PATH)
    df_fact_inventory = build_fact_inventory(data, OUTPUT_PATH)
    df_fact_OrderMarketing = build_fact_OrderMarketing(data, OUTPUT_PATH)
    df_fact_shipment = build_fact_shipment(data, OUTPUT_PATH)
    df_fact_payment = build_fact_payment(data, OUTPUT_PATH)
    df_fact_order_item = build_fact_order_item(data, OUTPUT_PATH)
    #df_fact_orders = fact_orders.build(data, OUTPUT_PATH)

    print("✅ Pipeline completado. Archivos guardados en warehouse/")

if __name__ == "__main__":
    run_pipeline()
