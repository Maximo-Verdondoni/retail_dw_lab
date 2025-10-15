# etl/transform/build_fact_order.py
import pandas as pd

def build_fact_order(data, output_path):
    """
    Genera una tabla de hechos order con columnas:
    id, order_id, customer_id, campaign_id, order_status, order_date_id, order_time,
    customer_billing_address_id, customer_shipping_address_id, currency, shipping_amount,
    order_subtotal, order_discount, order_tax, order_total, created_at_date, created_at_time
    """
    fact_order = data["orders"].copy()

    # Cargamos dim_calendar
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))

    # 1- Separamos order_date en fecha y hora
    fact_order['order_date'] = pd.to_datetime(fact_order['order_date'])
    fact_order['order_date_id'] = fact_order['order_date'].dt.date.map(calendar_map).astype("Int64")
    fact_order['order_time'] = fact_order['order_date'].dt.time
    fact_order = fact_order.drop(columns=['order_date'])

    # 2- Separamos created_at en fecha y hora
    fact_order['created_at'] = pd.to_datetime(fact_order['created_at'])
    fact_order['created_at_date'] = fact_order['created_at'].dt.date.map(calendar_map).astype("Int64")
    fact_order['created_at_time'] = fact_order['created_at'].dt.time
    fact_order = fact_order.drop(columns=['created_at'])

    # 3- Traemos campaign_id desde order_marketing
    order_marketing = data["order_marketing"].copy()
    fact_order = fact_order.merge(
        order_marketing[['order_id', 'campaign_id']],
        on='order_id',
        how='left'
    )
    fact_order['campaign_id'] = fact_order['campaign_id'].astype("Int64")

    # 4- Renombramos billing y shipping address
    fact_order = fact_order.rename(
        columns={
            'billing_address_id': 'customer_billing_address_id',
            'shipping_address_id': 'customer_shipping_address_id'
        }
    )
    # Convertimos a Int64 nullable para evitar floats con NaN
    fact_order['customer_billing_address_id'] = fact_order['customer_billing_address_id'].astype("Int64")
    fact_order['customer_shipping_address_id'] = fact_order['customer_shipping_address_id'].astype("Int64")

    # 5- Creamos ID surrogate key
    fact_order['id'] = range(1, len(fact_order) + 1)

    # 6- Nos quedamos solo con las columnas deseadas y en orden
    cols = [
        'id', 'customer_id', 'campaign_id', 'order_status',
        'order_date_id', 'order_time',
        'customer_billing_address_id', 'customer_shipping_address_id',
        'currency', 'shipping_amount', 'order_subtotal', 'order_discount',
        'order_tax', 'order_total', 'created_at_date', 'created_at_time'
    ]
    fact_order = fact_order[cols]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_order.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_order.to_csv(file_path, index=False)
    
    print(f"✅ fact_order guardado en {file_path}")
    return fact_order
