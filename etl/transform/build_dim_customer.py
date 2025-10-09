# etl/transform/build_dim_customer.py
import pandas as pd

def build_dim_customer(data: dict, output_path):
    """
    Construye la dimensión de clientes desnormalizando created_at relacionandolo con dim_calendar
    Guarda el resultado en warehouse/dim/dim_customer.csv
    """
    dim_customer = data["customers"].copy()
    
    #------------------- 

    # Cargamos dimension calendario
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    # Creamos diccionario fecha → calendar_id
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))
    
    #1- Separamos created_at en fecha y hora
    dim_customer['created_at'] = pd.to_datetime(dim_customer['created_at'])
    dim_customer['created_at_date'] = dim_customer['created_at'].dt.date
    dim_customer['created_at_time'] = dim_customer['created_at'].dt.time
    dim_customer['created_at_date_id'] = dim_customer['created_at_date'].map(calendar_map).astype("Int64")
    
    # Reordenamos columnas
    cols = [
        "customer_id", "first_name", "last_name", "email", "phone", "gender",
        "birth_date", "created_at_date_id",
        "created_at_time", "marketing_opt_in"
    ]
    dim_customer = dim_customer[cols]


    # -------------------

    #Cargamos customer_addreses 
    addresses = data["customer_addresses"].copy()

    #Hacemos el join
    dim_customer = pd.merge(
        dim_customer,
        addresses,
        on='customer_id',
        how='left'
    )
    cols = [
        "customer_id", "first_name", "last_name", "email", "phone", "gender",
        "birth_date", "created_at_date_id", "created_at_time", "marketing_opt_in",
        "address_type", "street", "city", "province", "country_code", "postal_code",
        "created_at", "is_primary"
    ]
    dim_customer = dim_customer[cols]

    #Separamos created_at_address en fecha y hora
    dim_customer['created_at'] = pd.to_datetime(dim_customer['created_at'])
    dim_customer['created_at_date_address'] = dim_customer['created_at'].dt.date
    dim_customer['created_at_time_address'] = dim_customer['created_at'].dt.time
    dim_customer = dim_customer.drop(columns=['created_at'])

    #Reordenamos columnas
    cols = [
        "customer_id", "first_name", "last_name", "email", "phone", "gender",
        "birth_date", "created_at_date_id", "created_at_time", "marketing_opt_in",
        "address_type", "street", "city", "province", "country_code", "postal_code",
        "created_at_date_address", "created_at_time_address", "is_primary"
    ]
    dim_customer = dim_customer[cols]    
    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_customers.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_customer.to_csv(file_path, index=False)
    
    print(f"✅ dim_customers guardado en {file_path}")
    return dim_customer
