import pandas as pd
def desnormalizar_categorias_productos(df_categories, df_products):
    """
    Desnormaliza las tablas de categorías y productos, eliminando category_id
    y reemplazándolo por category_name y parent_category_name.
    """
    
    # Crear copias para no modificar los originales
    categories = df_categories.copy()
    products = df_products.copy()
    
    # 1. Procesar las categorías para obtener los nombres de los padres
    # Renombrar la tabla de categorías para el join
    parent_categories = categories[['category_id', 'category_name']].rename(
        columns={'category_id': 'parent_category_id', 'category_name': 'parent_category_name'}
    )
    
    # Unir categorías con sus padres
    categories_completas = pd.merge(
        categories,
        parent_categories,
        on='parent_category_id',
        how='left'
    )
    
    # 2. Unir productos con categorías desnormalizadas
    products_desnormalizados = pd.merge(
        products,
        categories_completas[['category_id', 'category_name', 'parent_category_name']],
        on='category_id',
        how='left'
    )
    
    # 3. Eliminar la columna category_id
    products_desnormalizados = products_desnormalizados.drop('category_id', axis=1)
    
    return products_desnormalizados

# Cargar datos
categories = pd.read_csv('dw/categories.csv')
products = pd.read_csv('dw/products.csv')

# Desnormalizar
dim_products = desnormalizar_categorias_productos(categories, products)

# Guardar si es necesario
dim_products.to_csv('dw/dim_products.csv', index=False)
print(dim_products.head(10))
