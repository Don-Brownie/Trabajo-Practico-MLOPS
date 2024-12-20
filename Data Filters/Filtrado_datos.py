import pandas as pd
from datetime import datetime
import boto3
import os
from io import StringIO

# Configurar el cliente de S3
s3_client = boto3.client('s3', region_name='us-east-1')

# Función para leer archivos CSV desde S3
def read_s3_csv(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(obj['Body'])

# Definir funciones de filtrado
def filter_ads_views(ads_views, advertiser_ids):
    df = ads_views[ads_views['advertiser_id'].isin(advertiser_ids)]
    df = df[df['date'] == datetime.today().strftime('%Y-%m-%d')]
    return df

def filter_product_views(product_views, advertiser_ids):
    advertisers = advertiser_ids['advertiser_id'].tolist()
    df = product_views[product_views['advertiser_id'].isin(advertisers)]
    df = df[df['date'] == datetime.today().strftime('%Y-%m-%d')]
    return df

# Función para guardar los archivos filtrados en el sistema local de EC2
def save_to_ec2(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

# Parámetros para los archivos S3
bucket_name = 'grupo-17-mlops-bucket'
ads_views_key = 'ads_views.csv'
advertiser_ids_key = 'advertiser_ids.csv'
product_views_key = 'product_views.csv'

# Cargar los archivos CSV desde S3
ads_views = read_s3_csv(bucket_name, ads_views_key)
advertiser_ids = read_s3_csv(bucket_name, advertiser_ids_key)
product_views = read_s3_csv(bucket_name, product_views_key)

# Filtrar los datos
ads_views_filtered = filter_ads_views(ads_views, advertiser_ids)
product_views_filtered = filter_product_views(product_views, advertiser_ids)

# Guardar los resultados filtrados en el sistema de archivos local de EC2
output_ads_views_path = '/home/ubuntu/Trabajo-Practico-MLOPS/Datos_filtrados/ads_views_filtered.csv'
output_product_views_path = '/home/ubuntu/Trabajo-Practico-MLOPS/Datos_filtrados/product_views_filtered.csv'

save_to_ec2(ads_views_filtered, output_ads_views_path)
save_to_ec2(product_views_filtered, output_product_views_path)

def run_filtrado():
    # Configurar S3 y parámetros
    bucket_name = 'grupo-17-mlops-bucket'
    ads_views_key = 'ads_views.csv'
    advertiser_ids_key = 'advertiser_ids.csv'
    product_views_key = 'product_views.csv'

    # Leer datos de S3
    ads_views = read_s3_csv(bucket_name, ads_views_key)
    advertiser_ids = read_s3_csv(bucket_name, advertiser_ids_key)
    product_views = read_s3_csv(bucket_name, product_views_key)

    # Filtrar datos
    filtered_ads = filter_ads_views(ads_views, advertiser_ids)
    filtered_products = filter_product_views(product_views, advertiser_ids)

    # Guardar resultados en EC2
    save_to_ec2(filtered_ads, '/tmp/filtered_ads.csv')
    save_to_ec2(filtered_products, '/tmp/filtered_products.csv')


print("Archivos filtrados guardados en EC2.")

