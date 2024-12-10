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

def read_s3_csv(bucket_name, file_key):
    s3_client = boto3.client('s3', region_name='us-east-1')
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(obj['Body'])

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

def save_to_s3(df, s3_path):
    # Configuración
    bucket_name = 'grupo-17-mlops-bucket'
    region = 'us-east-1'

    # Crear cliente de S3
    s3_client = boto3.client('s3', region_name=region

    # Subir archivo a S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=df)
    print("Archivo subido exitosamente a s3")

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

    # Guardar resultados en S3
    save_to_s3('filtered_ads.csv', filtered_ads)
    save_to_s3('filtered_products.csv', filtered_products)
    
    print("Archivos filtrados guardados en S3")

# Función para calcular TopCTR
def calculate_top_ctr(**kwargs):
    bucket_name = 'grupo-17-mlops-bucket'
    filtered_ads_key = 'filtered_ads.csv'
    ads_views = read_s3_csv(bucket_name, filtered_ads_key)

    # Calcular métricas
    clicks = ads_views[ads_views['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')
    impressions = ads_views[ads_views['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')

    # Unir y calcular CTR
    stats = pd.merge(impressions, clicks, on=['advertiser_id', 'product_id'], how='left').fillna(0)
    stats['ctr'] = stats['clicks'] / stats['impressions']
    top_ctr = stats.sort_values(['advertiser_id', 'ctr'], ascending=[True, False]).groupby('advertiser_id').head(20)

    # Agregar una columna que indica la fecha
    top_ctr['date'] = datetime.today().strftime('%Y-%m-%d')

    # Guardar resultados
    save_to_s3('top_ctr.csv', top_ctr)
    print("Tarea finalizada correctamente")	

# Función para calcular TopProduct
def calculate_top_product(**kwargs):   
    bucket_name = 'grupo-17-mlops-bucket'
    filtered_prodcuts_key = 'filtered_products.csv'
    product_views = read_s3_csv(bucket_name, filtered_prodcuts_key)

    # Calcular productos más vistos
    top_product = product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
    top_product = top_product.sort_values(by='views', ascending=False).groupby('advertiser_id').head(20)

    #Agrgar una columna que indica la fecha
    top_product['date'] = datetime.today().strftime('%Y-%m-%d')
        
    # Guardar resultados
    save_to_s3('top_product.csv', top_product)
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
    bucket_name = 'grupo-17-mlops-bucket'
    top_ctr = read_s3_csv(bucket_name, 'top_ctr.csv')
    top_product = read_s3_csv(bucket_name, 'top_product.csv')
    
    # Conectar a la base de datos
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Crear tablas si no existen
    cur.execute("""CREATE TABLE IF NOT EXISTS top_ctr (
        advertiser_id VARCHAR(50),
        product_id VARCHAR(50),
        impressions INT,
        clicks INT,
        ctr FLOAT,
        date DATE
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS top_product (
        advertiser_id VARCHAR(50),
        product_id VARCHAR(50),
        views INT,
        date DATE
    );""")

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
