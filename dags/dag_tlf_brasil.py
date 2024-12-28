from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import scripts.etl_tlf_brasil as etlTlfBrasil
from airflow import DAG


def execute_script(**kwargs):
    try:
        etlTlfBrasil.main()
    except Exception as e:
        print(f"A Tranferencia dos dados TLF_Brasil falhou com o erro: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        dag_id='dag_tlf_brasil',
        dag_display_name='ETL TLF_Brasil',
        default_args=default_args,
        description='ETL do TLF_Brasil (Diana > Fractal) v1.0.2',
        schedule_interval='0 6 * * *',
        start_date=datetime(2023, 8, 29),
        catchup=False) as dag:

    run_script = PythonOperator(
        task_id='run_etl_tlf_brasil',
        python_callable=execute_script,
        provide_context=True
    )
