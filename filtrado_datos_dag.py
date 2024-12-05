from airflow import DAG
from airflow.operators.python import PythonOperator  # Nota: este es el módulo actualizado
from datetime import datetime
import sys

sys.path.append('/home/ubuntu/Trabajo-Practico-MLOPS')  

# Importar las funciones del script de filtrado
from filtrado_datos import filter_ads_views, filter_product_views, save_to_ec2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

with DAG(
    'filtrado_datos_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def ejecutar_filtrado():
        from filtrado_datos import ads_views, advertiser_ids, product_views  
        ads_views_filtered = filter_ads_views(ads_views, advertiser_ids)
        product_views_filtered = filter_product_views(product_views, advertiser_ids)

        # Guardar resultados en EC2
        output_ads_views_path = '/home/ubuntu/Trabajo-Practico-MLOPS/Datos_filtrados/ads_views_filtered.csv'  
        output_product_views_path = '/home/ubuntu/Trabajo-Practico-MLOPS/Datos_filtrados/product_views_filtered.csv'  

        save_to_ec2(ads_views_filtered, output_ads_views_path)
        save_to_ec2(product_views_filtered, output_product_views_path)

        print("Filtrado completado y datos guardados en EC2.")

    tarea_filtrar_datos = PythonOperator(
        task_id='filtrar_datos_task',
        python_callable=ejecutar_filtrado,
    )

