from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="import_airflow_connections",
    dag_display_name="Import Airflow Connections",
    default_args=default_args,
    schedule_interval=None,  # Run only once
    catchup=False,
) as dag:

    # Define the Bash command to import connections from a JSON file
    import_connections = BashOperator(
        task_id="import_connections",
        bash_command="cd /opt/airflow && airflow connections import /opt/airflow/config/connections.json",
    )

    # Task flow definition
    import_connections  # type: ignore
