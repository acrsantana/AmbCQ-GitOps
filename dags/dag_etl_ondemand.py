from datetime import datetime

import scripts.etl_centerline as etlCenterline
import scripts.etl_equipamento as etlEquipamento
import scripts.etl_estruturante as etlEstruturante
import scripts.etl_osp as etlOsp
import scripts.etl_sagre as etlSagre
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from scripts.utils.db_connection import DBConnection
from scripts.utils.outside_the_border import Otb

from airflow import DAG


def verf_conn_db():
    conn = DBConnection()

    print("Conectando com os bancos de dados...")
    try:
        conn.getEngine("diana_trino")
    except Exception as e:
        print(f"Erro ao se conectar com Trino: {e}")
        raise
    try:
        conn.getEngine("diana_raw_outside_plant")
    except Exception as e:
        print(f"Erro ao se conectar com Diana: {e}")
        raise
    try:
        conn.getEngine("fractal_db")
    except Exception as e:
        print(f"Erro ao se conectar com Fractal: {e}")
        raise


def sites_in_other_cities(**kwargs):
    dag_run = kwargs.get("dag_run")
    if dag_run:
        id = dag_run.conf.get("id_city", "No ID provided")
        otb = Otb()
        df = otb.getOutOfBorderSites(id)
        return df.to_dict(orient="records")


# Função para executar o script com o ID recebido
def exec_etl_sagre(**kwargs):
    # Obtém o ID da configuração da execução da DAG
    dag_run = kwargs.get("dag_run")
    if dag_run:
        id = dag_run.conf.get("id_city", "No ID provided")
        ti = kwargs["ti"]
        cities = ti.xcom_pull(task_ids="find_sites_in_other_cities")

        print(f"Executando ETL Sagre com o ID: {id}")
        try:
            etlSagre.main(id_city=id, cities=cities)
        except Exception as e:
            print(f"ETL Sagre falhou com o erro: {e}")
            raise
    else:
        print("Nenhum ID recebido.")


def exec_etl_estruturante(**kwargs):
    # Obtém o ID da configuração da execução da DAG
    dag_run = kwargs.get("dag_run")
    if dag_run:
        id = dag_run.conf.get("id_city", "No ID provided")
        print(f"Executando ETL Estruturante com o ID: {id}")
        try:
            etlEstruturante.main(id_city=id)
        except Exception as e:
            print(f"ETL Estruturante falhou com o erro: {e}")
            raise
    else:
        print("Nenhum ID recebido.")


def exec_etl_equipamento(**kwargs):
    # Obtém o ID da configuração da execução da DAG
    dag_run = kwargs.get("dag_run")
    if dag_run:
        id = dag_run.conf.get("id_city", "No ID provided")
        print(f"Executando ETL Equipamentos com o ID: {id}")
        try:
            etlEquipamento.main(id_city=id)
        except Exception as e:
            print(f"ETL Equipamentos falhou com o erro: {e}")
            raise
    else:
        print("Nenhum ID recebido.")


def exec_etl_centerline(**kwargs):
    # Obtém o ID da configuração da execução da DAG
    dag_run = kwargs.get("dag_run")
    if dag_run:
        id = dag_run.conf.get("id_city", "No ID provided")
        ti = kwargs["ti"]
        cities = ti.xcom_pull(task_ids="find_sites_in_other_cities")

        print(f"Executando ETL Centerline com o ID: {id}")
        try:
            etlCenterline.main(id_city=id, cities=cities)
        except Exception as e:
            print(f"ETL Centerline falhou com o erro: {e}")
            raise
    else:
        print("Nenhum ID recebido.")


def exec_etl_osp(**kwargs):
    # Obtém o ID da configuração da execução da DAG
    dag_run = kwargs.get("dag_run")
    if dag_run:
        id = dag_run.conf.get("id_city", "No ID provided")
        ti = kwargs["ti"]
        cities = ti.xcom_pull(task_ids="find_sites_in_other_cities")

        print(f"Executando ETL OSP com o ID: {id}")
        try:
            etlOsp.main(id_city=id, cities=cities)
        except Exception as e:
            print(f"ETL OSP falhou com o erro: {e}")
            raise
    else:
        print("Nenhum ID recebido.")


import logging


def verificar_sucesso(**kwargs):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    conn = DBConnection()

    dag_run = kwargs.get("dag_run")
    if dag_run:
        id_city = dag_run.conf.get("id_city", "No ID provided")
        logger.info(f"ID da cidade: {id_city}")

    # Obtendo as instâncias das tasks
    task_instances = kwargs["dag_run"].get_task_instances()

    houve_falha = False

    for task_instance in task_instances:
        if (
            task_instance.state == "failed"
        ):
            houve_falha = True
            try:
                fractal_engine = conn.getEngine('fractal_db')
                with fractal_engine.connect() as connection:
                    result = fractal_engine.execute(
                        f"UPDATE city SET status = 'Falha no cadastro' WHERE id = {id_city};"
                    )
                    logger.info(
                        "Update realizado com sucesso na tabela city para status 3"
                    )
            except Exception as e:
                logger.error(f"Erro ao executar o update na tabela city: {e}")
            break

    if not houve_falha:
        try:
            fractal_engine = conn.getEngine('fractal_db')
            if conn.is_connection_active(fractal_engine):
                result = fractal_engine.execute(
                    f"UPDATE city SET status = 'Disponível' WHERE id = {id_city};"
                )
                logger.info("Update realizado com sucesso na tabela city para status 2")
        except Exception as e:
            logger.error(f"Erro ao executar o update na tabela city: {e}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 8),
    "retries": 0,
}

dag = DAG(
    dag_id="etl_ondemand",
    dag_display_name="ETL Ondemand",
    description="ETL Ondemand (Diana > Fractal)  v1.1.0",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

verficar_conexao_banco = PythonOperator(
    task_id="verficar_conexao_banco",
    python_callable=verf_conn_db,
    provide_context=True,
    dag=dag,
)

find_sites_in_other_cities = PythonOperator(
    task_id="find_sites_in_other_cities",
    python_callable=sites_in_other_cities,
    provide_context=True,
    dag=dag,
)

execute_etl_sagre = PythonOperator(
    task_id="etl_sagre",
    python_callable=exec_etl_sagre,
    provide_context=True,
    dag=dag,
)

execute_etl_estruturante = PythonOperator(
    task_id="etl_estruturante",
    python_callable=exec_etl_estruturante,
    provide_context=True,
    dag=dag,
)

execute_etl_equipamento = PythonOperator(
    task_id="etl_equipamento",
    python_callable=exec_etl_equipamento,
    provide_context=True,
    dag=dag,
)

execute_etl_centerline = PythonOperator(
    task_id="etl_centerline",
    python_callable=exec_etl_centerline,
    provide_context=True,
    dag=dag,
)

execute_etl_osp = PythonOperator(
    task_id="etl_osp",
    python_callable=exec_etl_osp,
    provide_context=True,
    dag=dag,
)

verificar_task = PythonOperator(
    task_id="verificar_sucesso",
    python_callable=verificar_sucesso,
    provide_context=True,
    trigger_rule="all_done",
    dag=dag,
)

# Definindo a ordem de execução das tarefas
verficar_conexao_banco >> execute_etl_estruturante # type: ignore
execute_etl_estruturante >> [find_sites_in_other_cities, execute_etl_equipamento] # type: ignore
[find_sites_in_other_cities] >> execute_etl_sagre # type: ignore
[find_sites_in_other_cities] >> execute_etl_centerline # type: ignore
[find_sites_in_other_cities] >> execute_etl_osp # type: ignore
[
    execute_etl_sagre,
    execute_etl_centerline,
    execute_etl_osp,
    execute_etl_equipamento,
] >> verificar_task # type: ignore
