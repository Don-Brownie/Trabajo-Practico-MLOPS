from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Path para carpeta actual donde está el archivo pipeline_dag.py
sys.path.append(str(Path(__file__).resolve().parent))

# Importar las funciones de los scripts en la misma carpeta
from Filtrado_datos import run_filtrado
from Top_CTR import run_top_ctr
from Top_Product import run_top_product
from DBWriting import run_db_writing

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_recomendaciones',
    default_args=default_args,
    description='Pipeline para procesamiento de datos y generación de recomendaciones',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 6),
    catchup=False,
) as dag:
    
    # Tarea 1: Filtrar datos
    def filtrar_datos():
        run_filtrado()

    tarea_filtrar = PythonOperator(
        task_id='filtrar_datos',
        python_callable=filtrar_datos,
    )

    # Tarea 2: Computar TopCTR
    def computar_top_ctr():
        run_top_ctr()

    tarea_top_ctr = PythonOperator(
        task_id='computar_top_ctr',
        python_callable=computar_top_ctr,
    )

    # Tarea 3: Computar TopProduct
    def computar_top_product():
        run_top_product()

    tarea_top_product = PythonOperator(
        task_id='computar_top_product',
        python_callable=computar_top_product,
    )

    # Tarea 4: Escribir en la base de datos
    def escribir_db():
        run_db_writing()

    tarea_db_writing = PythonOperator(
        task_id='escribir_db',
        python_callable=escribir_db,
    )

    # Configurar dependencias
    tarea_filtrar >> [tarea_top_ctr, tarea_top_product] >> tarea_db_writing
