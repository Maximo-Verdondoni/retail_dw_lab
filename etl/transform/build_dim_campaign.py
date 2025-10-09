# etl/transform/build_dim_campaign.py
import pandas as pd

def build_dim_campaign(data: dict, output_path):
    """
    Construye la dimensión de campaña desnormalizando start_date y end_date relacionandolo con dim_calendar
    Guarda el resultado en warehouse/dim/dim_customer.csv
    """
    dim_campaign = data["campaigns"].copy()
    
    #------------------- 

    # Cargamos dimension calendario
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    # Creamos diccionario fecha → calendar_id
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))

    # Convertimos las columnas a datetime.date
    dim_campaign['start_date'] = pd.to_datetime(dim_campaign['start_date']).dt.date
    dim_campaign['end_date'] = pd.to_datetime(dim_campaign['end_date']).dt.date
    
    #1- Reemplazamos fechas con id
    dim_campaign['start_date_id'] = dim_campaign['start_date'].map(calendar_map).astype("Int64")
    dim_campaign['end_date_id'] = dim_campaign['end_date'].map(calendar_map).astype("Int64")
    dim_campaign = dim_campaign.drop(columns=['start_date', 'end_date'])
    
    # -------------------
    # Unimos con channels
    channels = data["channels"].copy()
    dim_campaign = pd.merge(
        dim_campaign,
        channels,
        on="channel_id",
        how="left"
    )
    dim_campaign = dim_campaign.drop(columns=["channel_id"])

    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_campaign.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_campaign.to_csv(file_path, index=False)
    
    print(f"✅ dim_campaign guardado en {file_path}")
    return dim_campaign
