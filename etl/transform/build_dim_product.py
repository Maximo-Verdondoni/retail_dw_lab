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
    
    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_products.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_products.to_csv(file_path, index=False)
    
    print(f"✅ dim_products guardado en {file_path}")
    return dim_products
