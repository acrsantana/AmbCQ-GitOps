import logging
import sys
import time

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .utils.db_connection import DBConnection


def main(**kwargs):

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    conn = DBConnection()
    diana_engine = conn.getEngine("diana_raw_outside_plant")
    fractal_engine = conn.getEngine("fractal_db")

    if conn.is_connection_active(fractal_engine):
        with fractal_engine.begin() as trans:  # type: ignore
            queryCreate = """
                CREATE TABLE IF NOT EXISTS osp_lance_cabo_optico_tlf_brasil (
                    banco_dados_osp varchar(255) DEFAULT NULL,
                    lance_cabo_optico_id varchar(255) DEFAULT NULL,
                    lance_cabo_optico_situacao varchar(255) DEFAULT NULL,
                    lance_cabo_optico_drop varchar(255) DEFAULT NULL,
                    lance_cabo_optico_rota varchar(255) DEFAULT NULL,
                    lance_cabo_optico_cd varchar(255) DEFAULT NULL,
                    tipo_lance_cabo_optico_id varchar(255) DEFAULT NULL,
                    tipo_lance_cabo_optico_qt varchar(255) DEFAULT NULL,
                    tipo_lance_cabo_optico_tipo varchar(255) DEFAULT NULL,
                    tipo_lance_cabo_optico_cd varchar(255) DEFAULT NULL,
                    tipo_lance_cabo_optico_material varchar(255) DEFAULT NULL,
                    tipo_lance_cabo_optico_md_transmissao varchar(255) DEFAULT NULL,
                    lance_cabo_optico_cmp_estimado varchar(255) DEFAULT NULL,
                    lance_cabo_optico_cmp_real varchar(255) DEFAULT NULL,
                    lance_cabo_optico_cmp_real_sobra varchar(255) DEFAULT NULL,
                    lance_cabo_optico_instalacao_dt varchar(255) DEFAULT NULL,
                    ponto_acesso_inicio_id varchar(255) DEFAULT NULL,
                    ponto_acesso_inicio_tipo varchar(255) DEFAULT NULL,
                    ponto_acesso_fim_id varchar(255) DEFAULT NULL,
                    ponto_acesso_fim_tipo varchar(255) DEFAULT NULL,
                    lance_cabo_optico_propriedade_id varchar(255) DEFAULT NULL,
                    lance_cabo_optico_propriedade_ds varchar(255) DEFAULT NULL,
                    lance_cabo_optico_proprietario_id varchar(255) DEFAULT NULL,
                    lance_cabo_optico_proprietario_cd varchar(255) DEFAULT NULL,
                    lance_cabo_optico_proprietario_nm varchar(255) DEFAULT NULL,
                    cable_spam_id varchar(255) DEFAULT NULL,
                    tipo_instalacao_cabo_tipo_instalacao varchar(255) DEFAULT NULL,
                    tipo_instalacao_cabo_em_lance_duto varchar(255) DEFAULT NULL,
                    contagem_id varchar(255) DEFAULT NULL,
                    contagem_cabo_logico varchar(255) DEFAULT NULL,
                    contagem_situacao varchar(255) DEFAULT NULL,
                    contagem_tipo_contagem varchar(255) DEFAULT NULL,
                    ordem_trabalho_id varchar(255) DEFAULT NULL,
                    ordem_trabalho_cd varchar(255) DEFAULT NULL,
                    ordem_trabalho_dt_inicio varchar(255) DEFAULT NULL,
                    ordem_trabalho_dt_fim varchar(255) DEFAULT NULL,
                    cabo_logico_id varchar(255) DEFAULT NULL,
                    cabo_logico_funcao varchar(255) DEFAULT NULL,
                    cabo_logico_cd varchar(255) DEFAULT NULL,
                    cabo_logico_lateral_id varchar(255) DEFAULT NULL,
                    cabo_logico_lateral_lateral varchar(255) DEFAULT NULL,
                    cabo_logico_local_origem_cnl varchar(255) DEFAULT NULL,
                    cabo_logico_local_origem_nm varchar(255) DEFAULT NULL,
                    cabo_logico_local_origem_sigla varchar(255) DEFAULT NULL,
                    cabo_logico_site_origem_id varchar(255) DEFAULT NULL,
                    cabo_logico_site_origem_cd varchar(255) DEFAULT NULL,
                    cabo_logico_site_origem_tipo varchar(255) DEFAULT NULL,
                    cabo_logico_site_holder_origem_id varchar(255) DEFAULT NULL,
                    cabo_logico_site_holder_origem_cd varchar(255) DEFAULT NULL,
                    cabo_logico_site_holder_origem_simbolo varchar(255) DEFAULT NULL,
                    cabo_logico_local_destino_cnl varchar(255) DEFAULT NULL,
                    cabo_logico_local_destino_nm varchar(255) DEFAULT NULL,
                    cabo_logico_local_destino_sigla varchar(255) DEFAULT NULL,
                    cabo_logico_site_destino_id varchar(255) DEFAULT NULL,
                    cabo_logico_site_destino_cd varchar(255) DEFAULT NULL,
                    cabo_logico_site_destino_tipo varchar(255) DEFAULT NULL,
                    cabo_logico_site_holder_destino_id varchar(255) DEFAULT NULL,
                    cabo_logico_site_holder_destino_cd varchar(255) DEFAULT NULL,
                    cabo_logico_site_holder_destino_simbolo varchar(255) DEFAULT NULL,
                    name varchar(255) DEFAULT NULL,
                    geometry geometry,
                    dt_foto varchar(255) DEFAULT NULL
                );
            """
            logger.info("Iniciando criação da tabela osp_lance_cabo_optico_tlf_brasil.")
            fractal_engine.execute(queryCreate)
            # trans.commit()

    sagre_purge_query = text(f"""DELETE FROM osp_lance_cabo_optico_tlf_brasil;""")
    logger.info(f"Iniciando processo de expurgo.")
    try:
        if conn.is_connection_active(fractal_engine):
            result = fractal_engine.execute(sagre_purge_query)
            logger.info(f"Número de linhas expurgadas: {result.rowcount}")

    except Exception as e:
        logger.error(f"Erro ao executar DELETE: {e}")
        sys.exit(1)

    try:
        logger.info(f"Iniciando ETL TLF_Brasil.")
        page_size = 10000  # Defina o tamanho da página
        offset = 0
        num_rows = 0

        while True:
            query = f"""
            SELECT * FROM raw_outside_plant.osp_lance_cabo_optico 
            WHERE banco_dados_osp = 'TLF_BRASIL'
            LIMIT {page_size} OFFSET {offset};
            """
            logger.info(f"Iniciando query de extração de dados.")
            start_time = time.time()
            df_page = pd.read_sql(query, con=diana_engine)
            query_time = time.time() - start_time
            logger.info(f"Tempo de execução da query: {query_time:.2f} segundos")

            if df_page.empty:
                break

            start_time = time.time()
            df_page.to_sql(
                "osp_lance_cabo_optico_tlf_brasil",
                con=fractal_engine,
                if_exists="append",
                index=False,
            )
            insert_time = time.time() - start_time
            logger.info(f"Tempo de execução do insert: {insert_time:.2f} segundos")

            num_rows += len(df_page)
            offset += page_size

            logger.info(
                f"Processando página com offset {offset}. Total de linhas processadas até agora: {num_rows}"
            )

        if num_rows > 0:
            logger.info(
                f"Processamento do ETL finalizado.\nFoi realizada a carga de {num_rows} linhas."
            )
        else:
            logger.info(
                f"Processamento do ETL finalizado: A tabela não possui dados cadastrados."
            )

    except Exception as e:
        logger.error(f"Erro durante o processamento do ETL: {e}")
        sys.exit(1)

    sys.exit(0)
