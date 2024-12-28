import scripts.city_data_transfer as cityDataTransfer

from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

def execute_script(**kwargs):
    try:
        cityDataTransfer.main()
    except Exception as e:
        print(f"A Tranferencia de Cidade falhou com o erro: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        dag_id='dag_city_data_transfer',
        dag_display_name='City Data Transfer',
        default_args=default_args,
        description='ETL de Cidades (Diana > Fractal) v1.0.0',
        schedule_interval=timedelta(weeks=1),
        start_date=datetime(2023, 8, 7),
        catchup=False) as dag:

    run_script = PythonOperator(
        task_id='run_city_data_transfer',
        python_callable=execute_script,
        provide_context=True
    )
