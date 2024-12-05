import pandas as pd


def calcular_top_product(product_views, top_n=20):
    """
    Calcula los top productos más vistos para cada advertiser activo.

    Args:
        log_views (pd.DataFrame): DataFrame con columnas ['advertiser_id', 'product_id', 'fecha'].
        top_n (int): Número máximo de productos a devolver por advertiser.

    Returns:
        pd.DataFrame: DataFrame con columnas ['advertiser_id', 'product_id', 'views'].
    """
    # Contar vistas por advertiser y producto
    views = product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')

    # Ordenar por vistas y obtener los top productos por advertiser
    top_product = (
        views.sort_values(['advertiser_id', 'views'], ascending=[True, False])
        .groupby('advertiser_id')
        .head(top_n)
        .reset_index(drop=True)
    )

    return top_product[['advertiser_id', 'product_id', 'views']]