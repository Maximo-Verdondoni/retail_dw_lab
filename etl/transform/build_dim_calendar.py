# etl/transform/build_dim_calendar.py
import pandas as pd

def build_dim_calendar(output_path, start_date="2025-01-01", end_date="2025-12-31"):
    """
    Genera una tabla de dimensión calendario con campos:
    id, date, day, month, year, day_name, month_name, quarter
    """
    # Creamos rango de fechas
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'date': dates})

    # Agregamos columnas derivadas
    df['id'] = (df.index + 1).astype(int)
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day_name'] = df['date'].dt.day_name()
    df['month_name'] = df['date'].dt.month_name()
    df['quarter'] = df['date'].dt.quarter.map({1:'Q1', 2:'Q2', 3:'Q3', 4:'Q4'})

    # Reordenamos columnas
    dim_calendar = df[['id', 'date', 'day', 'month', 'year', 'day_name', 'month_name', 'quarter']]
    # Guardamos en warehouse/dim
    file_path = output_path / "dim" / "dim_calendar.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dim_calendar.to_csv(file_path, index=False)
    
    print(f"✅ dim_calendar guardado en {file_path}")
    return dim_calendar