import pandas as pd

def build(data, output_path):
    products = data["products"]
    categories = data["categories"]

    dim_products = products.merge(
        categories[["category_id", "category_name"]],
        on="category_id",
        how="left"
    )

    file_path = output_path / "dim_products.csv"
    dim_products.to_csv(file_path, index=False)

    print(f"dim_products guardado en {file_path}")
    return dim_products
