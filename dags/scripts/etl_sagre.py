import logging
import sys

import pandas as pd
from sqlalchemy import text
from unidecode import unidecode

from .utils.db_connection import DBConnection


def main(**kwargs):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    id_city = kwargs.get("id_city", None)
    logger.info(f"Buscando dados da cidade com ID: {id_city}")
    cities = pd.DataFrame.from_records(kwargs.get("cities", None))

    conn = DBConnection()
    diana_engine = conn.getEngine("diana_raw_outside_plant")
    fractal_engine = conn.getEngine("fractal_db")

    sagre_purge_query = text(f"""DELETE FROM sagre WHERE id_city = {id_city};""")
    logger.info(f"Iniciando processo de expurgo.")
    try:
        if conn.is_connection_active(fractal_engine):
            result = fractal_engine.execute(sagre_purge_query)
            logger.info(f"Total de linhas expurgadas: {result.rowcount}")
    except Exception as e:
        logger.error(f"Erro ao executar DELETE: {e}")
        sys.exit(1)

    for _, row in cities.iterrows():
        city = unidecode(row["city"]).replace(" ", "_").upper()
        logger.info(f"Iniciando processamento da cidade: {city}, {row['uf']}")
        logger.info(f"Buscando dados de Fibra Óptica da cidade.")
        df_diana = pd.read_sql(
            f"""
                            SELECT
                                scs.cable_ds as descricao,
                                scs.cable_length as comprimento,
                                scs.cable_type as tipo,
                                scs.cable_contagem_1 as contagem_1,
                                scs.cable_contagem_2 as contagem_2,
                                scs.cable_contagem_3 as contagem_3,
                                scs.cable_contagem_4 as contagem_4,
                                scs.cable_contagem_5 as contagem_5,
                                scs.cable_contagem_6 as contagem_6,
                                scs.cable_contagem_7 as contagem_7,
                                scs.cable_contagem_8 as contagem_8,
                                scs.cable_contagem_9 as contagem_9,
                                scs.cable_contagem_10 as contagem_10,
                                scs.cable_contagem_11 as contagem_11,
                                scs.cable_contagem_12 as contagem_12,
                                scs.cable_contagem_13 as contagem_13,
                                scs.cable_contagem_14 as contagem_14,
                                scs.cable_contagem_15 as contagem_15,
                                scs.cable_contagem_16 as contagem_16,
                                scs.cable_contagem_17 as contagem_17,
                                scs.cable_contagem_18 as contagem_18,
                                scs.cable_contagem_19 as contagem_19,
                                scs.cable_contagem_20 as contagem_20,
                                scs.cable_pares_mortos as pares_mortos,
                                scs.cable_capacidade as capacidade,
                                scs.cable_info_duto as info_duto,
                                scs.cable_pp as pp,
                                scs.cable_cc as cc,
                                scs.cable_ident_optico as ident_optico,
                                scs.cable_ocupacao as ocupacao,
                                scs.cable_feat_num as feat_num,
                                scs.cable_cable_id as cable_id,
                                scs.cable_situacao as situacao,
                                scs.geometry
                            FROM raw_outside_plant.sagre_cable_seg scs
                            WHERE uf_sg = '{row['uf']}' 
                            AND city_nm = '{city}'
                        """,
            con=diana_engine,
        )

        if not df_diana.empty:
            df_diana["id_city"] = id_city
            df_diana.to_sql(
                "sagre", con=fractal_engine, if_exists="append", index=False
            )
            num_rows = len(df_diana)
            logger.info(
                f"Processamento finalizado da cidade: {row['city']}.\nFoi realizada a carga de {num_rows} linhas."
            )
        else:
            logger.info(
                f"Processamento finalizado da cidade: {row['city']}.\nA cidade não possui dados cadastrados."
            )

    sys.exit(0)
