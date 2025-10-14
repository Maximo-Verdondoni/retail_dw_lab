# etl/transform/build_fact_payment.py
import pandas as pd
def build_fact_payment(data, output_path):
    """
    Genera una tabla de hechos payment con campos:
     id, customer_id, campaign_id, paid_at_date_id, paid_at_time, amount, currency, payment_method, payment_status,transaction_id
    """
    fact_payment = data["payments"].copy()

    # Cargamos dimension calendario
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    # Creamos diccionario fecha → calendar_id
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))
    
    #1- Separamos paid_at en fecha y hora
    fact_payment['paid_at'] = pd.to_datetime(fact_payment['paid_at'])
    fact_payment['paid_at_date'] = fact_payment['paid_at'].dt.date
    fact_payment['paid_at_time'] = fact_payment['paid_at'].dt.time
    fact_payment['paid_at_date_id'] = fact_payment['paid_at_date'].map(calendar_map).astype("Int64")


    #2- Cargamos orders y obtenemos customer_id
    orders = data["orders"].copy()
    fact_payment = fact_payment.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left"
    )

    #3- Cargamos order_marketing y obtenemos campaign_id
    order_marketing = data["order_marketing"].copy()
    fact_payment = fact_payment.merge(
        order_marketing[["order_id", "campaign_id"]],
        on="order_id",
        how="left"
    )

    #4- Creamos surrogate key
    fact_payment["id"] = range(1, len(fact_payment) + 1)

    cols = [
        "id", "customer_id", "campaign_id",
        "paid_at_date_id", "paid_at_time", "amount", "currency", "payment_method",
        "payment_status","transaction_id"
    ]
    fact_payment = fact_payment[cols]


    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_payment.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_payment.to_csv(file_path, index=False)
    
    print(f"✅ fact_payment guardado en {file_path}")
    return fact_payment