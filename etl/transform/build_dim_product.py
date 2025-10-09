# etl/transform/build_dim_products.py
import pandas as pd

def build(data: dict, output_path):
    """
    Construye la dimensión de productos desnormalizando categorías y productos.
    Guarda el resultado en warehouse/dim/dim_products.csv
    """
    categories = data["categories"].copy()
    products = data["products"].copy()
    
    parent_categories = categories[["category_id", "category_name"]].rename(
        columns={
            "category_id": "parent_category_id",
            "category_name": "parent_category_name"
        }
    )
    
    categories_completas = pd.merge(
        categories,
        parent_categories,
        on="parent_category_id",
        how="left"
    )
    
    dim_products = pd.merge(
        products,
        categories_completas[["category_id", "category_name", "parent_category_name"]],
        on="category_id",
        how="left"
    ).drop("category_id", axis=1)

    #------------------- 

    # Cargamos dimension calendario
    dim_calendar = pd.read_csv(output_path / "dim" / "dim_calendar.csv", parse_dates=["date"])
    # Creamos diccionario fecha → calendar_id
    calendar_map = dict(zip(dim_calendar['date'].dt.date, dim_calendar['id']))
    
    #1- Mapeamos active_from 
    dim_products['active_from'] = pd.to_datetime(dim_products['active_from']).dt.date
    dim_products['active_from_id'] = dim_products['active_from'].map(calendar_map).astype("Int64")
    dim_products = dim_products.drop(columns=['active_from'])

    #2- Mapeamos active_to
    dim_products['active_to'] = pd.to_datetime(dim_products['active_to']).dt.date
    dim_products['active_to_id'] = dim_products['active_to'].map(calendar_map).astype("Int64")
    dim_products = dim_products.drop(columns=['active_to'])

    
    #3- Separamos created_at en fecha y hora
    dim_products['created_at'] = pd.to_datetime(dim_products['created_at'])
    dim_products['created_at_date'] = dim_products['created_at'].dt.date
    dim_products['created_at_time'] = dim_products['created_at'].dt.time
    dim_products['created_at_date_id'] = dim_products['created_at_date'].map(calendar_map).astype("Int64")
    
    # Reordenamos columnas
    cols = [
        "product_id", "sku", "product_name", "brand", "unit_price", "unit_cost",
        "active_from_id", "active_to_id",
        "created_at_date_id", "created_at_time",
        "category_name", "parent_category_name"
    ]
    dim_products = dim_products[cols]

    
    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_products.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_products.to_csv(file_path, index=False)
    
    print(f"✅ dim_products guardado en {file_path}")
    return dim_products
