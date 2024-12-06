import pandas as pd


def calcular_top_ctr(ads_views, top_n=20):
    """
    Calcula los top productos por CTR para cada advertiser activo.

    Args:
        log_ads (pd.DataFrame): DataFrame con columnas ['advertiser_id', 'product_id', 'type', 'fecha'].
        top_n (int): Número máximo de productos a devolver por advertiser.

    Returns:
        pd.DataFrame: DataFrame con columnas ['advertiser_id', 'product_id', 'ctr'].
    """
    # Contar clics e impresiones por advertiser y producto
    clicks = ads_views[ads_views['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(
        name='clicks')
    impressions = ads_views[ads_views['type'] == 'impression'].groupby(
        ['advertiser_id', 'product_id']).size().reset_index(name='impressions')

    # Unir clics e impresiones
    stats = pd.merge(impressions, clicks, on=['advertiser_id', 'product_id'], how='left')
    stats['clicks'] = stats['clicks'].fillna(0)

    # Calcular CTR
    stats['ctr'] = stats['clicks'] / stats['impressions']

    # Ordenar por CTR y obtener los top productos por advertiser
    top_ctr = (
        stats.sort_values(['advertiser_id', 'ctr'], ascending=[True, False])
        .groupby('advertiser_id')
        .head(top_n)
        .reset_index(drop=True)
    )

    return top_ctr[['advertiser_id', 'product_id', 'ctr']]


def run_top_ctr():
    # Leer datos filtrados desde EC2
    filtered_ads = pd.read_csv('/tmp/filtered_ads.csv')
    top_ctr = (
        filtered_ads[filtered_ads['type'] == 'click']
        .groupby(['advertiser_id', 'product_id'])
        .size()
        .reset_index(name='click_count')
        .sort_values(by='click_count', ascending=False)
        .groupby('advertiser_id')
        .head(20)
    )
    save_to_ec2(top_ctr, '/tmp/top_ctr.csv')

