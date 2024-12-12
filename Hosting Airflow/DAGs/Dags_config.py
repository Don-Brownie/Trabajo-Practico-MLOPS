from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
from pathlib import Path
import pandas as pd
import psycopg2
import boto3
from io import StringIO

# Función para leer archivos CSV desde S3
def read_s3_csv(bucket_name, file_key):
    s3_client = boto3.client('s3', region_name='us-east-1')
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(obj['Body'])

# Definir funciones de filtrado
def filter_ads_views(ads_views, advertiser_ids):
    ads_views['date'] = pd.to_datetime(ads_views['date']).dt.date
    df = ads_views[ads_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]
    df = df[df['date'] == datetime.today().date()]
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

# Función para calcular TopCTR
def calculate_top_ctr(**kwargs):
    print("Iniciando tarea: Calcular TopCTR")
    download_path = '/tmp'
    ads_views = pd.read_csv(os.path.join(download_path, 'filtered_ads.csv'))

    # Calcular métricas
    clicks = ads_views[ads_views['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')
    impressions = ads_views[ads_views['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')

    # Unir y calcular CTR
    stats = pd.merge(impressions, clicks, on=['advertiser_id', 'product_id'], how='left').fillna(0)
    stats['ctr'] = stats['clicks'] / stats['impressions']
    
    # Ordenar por CTR y agrupar por advertiser_id y date
    stats = stats.sort_values(['ctr'], ascending=False)
    
    # Seleccionar solo los 20 mejores por cada día y por cada advertiser_id
    top_ctr = stats.head(20)
    
    # Resetear el índice para poder guardar el CSV
    top_ctr = top_ctr.reset_index(drop=True)
    
    # Agregar una columna con la fecha
    top_ctr['date'] = pd.Timestamp.now().date()

    # Guardar los resultados a un archivo CSV
    top_ctr.to_csv(os.path.join(download_path, 'top_ctr.csv'), index=False)
    
    print("Tarea finalizada correctamente")

# Función para calcular TopProduct
def calculate_top_product(**kwargs):
    print("Iniciando tarea: Calcular TopProduct")
    download_path = '/tmp'
    product_views = pd.read_csv(os.path.join(download_path, 'filtered_products.csv'))

    # Calcular productos más vistos
    top_product = product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')

    # Ordenar por views
    top_product = top_product.sort_values(by=['views'], ascending=False)
    
    # Seleccionar solo los 20 productos más vistos por cada día
    top_product = top_product.head(20)
    
    # Resetear el índice para poder guardar el CSV
    top_product = top_product.reset_index(drop=True)
    
    # Agregar una columna con la fecha
    top_product['date'] = pd.Timestamp.now().date()
    
    # Guardar los resultados a un archivo CSV
    top_product.to_csv(os.path.join(download_path, 'top_product.csv'), index=False)
    
    print("Tarea finalizada correctamente")

# Función para escribir en PostgreSQL
def write_to_postgres(**kwargs):
    print("Iniciando tarea: Escribir en PostgreSQL")
    download_path = '/tmp'
    db_config = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'yourpassword123',
        'host': 'grupo-17-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com',
        'port': 5432,
    }

    # Cargar datos
    top_ctr = pd.read_csv(os.path.join(download_path, 'top_ctr.csv'))
    top_product = pd.read_csv(os.path.join(download_path, 'top_product.csv'))

    top_ctr['date'] = pd.to_datetime(top_ctr['date'], errors='coerce').dt.date
    top_product['date'] = pd.to_datetime(top_product['date'], errors='coerce').dt.date

    # Conectar a la base de datos
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Insertar datos
    for _, row in top_ctr.iterrows():
        cur.execute(
            "INSERT INTO top_ctr (advertiser_id, product_id, impressions, clicks, ctr, date) VALUES (%s, %s, %s, %s, %s, %s)",
            tuple(row)
        )

    for _, row in top_product.iterrows():
        cur.execute(
            "INSERT INTO top_product (advertiser_id, product_id, views, date) VALUES (%s, %s, %s, %s)",
            tuple(row)
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Tarea finalizada correctamente")

# Configuración predeterminada del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'grupo17',
    default_args=default_args,
    description='Pipeline de procesamiento de datos y escritura en PostgreSQL',
    schedule='@daily',
    start_date=datetime(2024, 12, 7),
    catchup=False,
) as dag:

    filter_task = PythonOperator(
        task_id='filter_active_advertisers',
        python_callable=run_filtrado
    )

    top_ctr_task = PythonOperator(
        task_id='calculate_top_ctr',
        python_callable=calculate_top_ctr
    )

    top_product_task = PythonOperator(
        task_id='calculate_top_product',
        python_callable=calculate_top_product
    )

    db_writing_task = PythonOperator(
        task_id='write_to_postgres',
        python_callable=write_to_postgres
    )

    filter_task >> [top_ctr_task, top_product_task] >> db_writing_task
