from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
from pathlib import Path
import pandas as pd
import psycopg2

# Función para filtrar datasets
def filter_datasets(**kwargs):
    print("Iniciando tarea: Filtrar datasets")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'grupo-17-mlops-bucket'
    download_path = os.path.expanduser('~/airflow/tmp')
    Path(download_path).mkdir(parents=True, exist_ok=True)

    # Cargar archivos
    advertiser_ids = pd.read_csv(os.path.join(download_path, 'advertiser_ids.csv'))
    ads_views = pd.read_csv(os.path.join(download_path, 'ads_views.csv'))
    product_views = pd.read_csv(os.path.join(download_path, 'product_views.csv'))

    # Filtrar datos
    advertisers = advertiser_ids['advertiser_id'].tolist()
    ads_filtered = ads_views[ads_views['advertiser_id'].isin(advertisers)]
    products_filtered = product_views[product_views['advertiser_id'].isin(advertisers)]

    # Filtrar por fecha actual
    today = datetime.today().strftime('%Y-%m-%d')
    ads_filtered = ads_filtered[ads_filtered['date'] == today]
    products_filtered = products_filtered[products_filtered['date'] == today]

    # Guardar resultados
    ads_filtered.to_csv(os.path.join(download_path, 'ads_views_filtered.csv'), index=False)
    products_filtered.to_csv(os.path.join(download_path, 'product_views_filtered.csv'), index=False)
    print("Tarea finalizada correctamente")

# Función para calcular TopCTR
def calculate_top_ctr(**kwargs):
    print("Iniciando tarea: Calcular TopCTR")
    download_path = os.path.expanduser('~/airflow/tmp')
    ads_views = pd.read_csv(os.path.join(download_path, 'ads_views_filtered.csv'))

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
    top_ctr.to_csv(os.path.join(download_path, 'top_ctr.csv'), index=False)
    print("Tarea finalizada correctamente")

# Función para calcular TopProduct
def calculate_top_product(**kwargs):
    print("Iniciando tarea: Calcular TopProduct")
    download_path = os.path.expanduser('~/airflow/tmp')
    product_views = pd.read_csv(os.path.join(download_path, 'product_views_filtered.csv'))

    # Calcular productos más vistos
    top_product = product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
    top_product = top_product.sort_values(by='views', ascending=False).groupby('advertiser_id').head(20)

    #Agrgar una columna que indica la fecha
    top_product['date'] = datetime.today().strftime('%Y-%m-%d')
        
    # Guardar resultados
    top_product.to_csv(os.path.join(download_path, 'top_product.csv'), index=False)
    print("Tarea finalizada correctamente")

# Función para escribir en PostgreSQL
def write_to_postgres(**kwargs):
    print("Iniciando tarea: Escribir en PostgreSQL")
    download_path = os.path.expanduser('~/airflow/tmp')
    db_config = {
        'dbname': 'postgres',
        'user': 'grupo-17-rds',
        'password': 'h$vEy0)$',
        'host': 'grupo-17-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com',
        'port': 5432,
    }

    # Cargar datos
    top_ctr = pd.read_csv(os.path.join(download_path, 'top_ctr.csv'))
    top_product = pd.read_csv(os.path.join(download_path, 'top_product.csv'))

    # Conectar a la base de datos
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Crear tablas si no existen
    cur.execute("""CREATE TABLE IF NOT EXISTS top_ctr (
        product_id VARCHAR(50),
        advertiser_id VARCHAR(50),
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
            "INSERT INTO top_ctr (product_id, advertiser_id, impressions, clicks, ctr, date) VALUES (%s, %s, %s, %s, %s, %s)",
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
    'grupo17_v2',
    default_args=default_args,
    description='Pipeline de procesamiento de datos y escritura en PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 7),
    catchup=False,
) as dag:

    filter_task = PythonOperator(
        task_id='filter_active_advertisers',
        python_callable=filter_datasets
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
