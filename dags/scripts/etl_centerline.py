import logging
import sys

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .utils.db_connection import DBConnection


def main(**kwargs):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    id_city = kwargs.get("id_city", None)
    cities = pd.DataFrame.from_records(kwargs.get("cities", None))

    conn = DBConnection()

    diana_engine = conn.getEngine("diana_raw_outside_plant")

    fractal_engine = conn.getEngine("fractal_db")

    purge_query = text(f"""DELETE FROM centerline WHERE id_city = {id_city};""")

    logger.info(f"Iniciando processo de expurgo.")
    try:
        if conn.is_connection_active(fractal_engine):
            result = fractal_engine.execute(purge_query)
            logger.info(f"Total de linhas expurgadas: {result.rowcount}")

    except Exception as e:
        logger.error(f"Erro ao executar DELETE: {e}")
        sys.exit(1)

    for _, row in cities.iterrows():
        logger.info(f"Iniciando processamento da cidade: {row['city']}, {row['uf']}")
        logger.info(f"Buscando dados do Open Street Map da cidade.")
        df_diana = pd.read_sql(
            f"""
                            SELECT 
                                osmr.SIGLA_UF as sigla_uf,
                                osmr.CD_MUN as cd_mun,
                                osmr.NM_MUN as nm_mun,
                                osmr.layer,
                                osmr.bridge,
                                osmr.tunnel,
                                osmr.osm_id as cod_trecho,
                                osmr.name as log,
                                osmr.fclass,
                                osmr.geometry  
                            FROM raw_outside_plant.open_street_map_road osmr 
                            WHERE osmr.CD_MUN = '{row['code']}' 
                            AND osmr.fclass not in (
                                    'bridleway',
                                    'busway',
                                    'cycleway',
                                    'footway',
                                    'living_street',
                                    'path',
                                    'pedestrian',
                                    'service',
                                    'steps',
                                    'track_grade1',
                                    'track_grade2',
                                    'track_grade3',
                                    'track_grade4',
                                    'track_grade5',
                                    'unknown'
                                )
                        """,
            con=diana_engine,
        )

        logger.info(f"Realizando a carga dos dados no banco de dados.")
        if not df_diana.empty:
            df_diana["id_city"] = id_city
            df_diana.to_sql(
                "centerline", con=fractal_engine, if_exists="append", index=False
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
