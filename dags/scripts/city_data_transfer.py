import logging
import sys

import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .utils.db_connection import DBConnection


def main(**kwargs):
    # Configurando o logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Iniciando Script ETL de Cidades (Diana > Fractal)")

    logger.info("Verificação da conectividade com os bancos.")

    conn = DBConnection()

    diana_engine = conn.getEngine("diana_raw_outside_plant")

    fractal_engine = conn.getEngine("fractal_db")

    logger.info("Executando querys de exportação...")

    df_diana = pd.read_sql(
        "SELECT * FROM code_city_ibge_cnl_osp ORDER BY uf_code, municipio_code",
        con=diana_engine,
    )

    df_city_reference = pd.read_sql(
        "SELECT * FROM city_reference ORDER BY uf_code, municipio_code",
        con=fractal_engine,
    )
    df_city_reference.drop(columns=["id"], inplace=True)

    logger.info("Iniciando a carga de dados.")
    if df_city_reference.empty:
        df_diana.to_sql(
            "city_reference", con=fractal_engine, if_exists="append", index=False
        )
        num_rows = len(df_diana)
        logger.info(
            f"Processamento finalizado. Foi realizada a carga de {num_rows} linhas."
        )
        sys.exit(0)
    else:
        df_divergencias = (
            df_diana.merge(df_city_reference, how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns="_merge")
        )

        # with fractal_engine.connect() as conn:
        if conn.is_connection_active(fractal_engine):
            cont_insert = 0
            cont_update = 0
            with fractal_engine.begin() as trans:  # type: ignore
                for _, row in df_divergencias.iterrows():
                    check_sql = text(
                        """
                        SELECT COUNT(*) 
                        FROM city_reference 
                        WHERE municipio_code = :municipio_code
                    """
                    )
                    result = fractal_engine.execute(
                        check_sql, {"municipio_code": row["municipio_code"]}
                    )
                    exists = result.scalar() > 0

                    if exists:
                        update_sql = text(
                            """
                            UPDATE city_reference
                            SET {}
                            WHERE municipio_code = :municipio_code
                        """.format(
                                ", ".join(
                                    f"{col} =:{col}"
                                    for col in df_divergencias.columns
                                    if col != "municipio_code"
                                )
                            )
                        )
                        params = {
                            col: row[col]
                            for col in df_divergencias.columns
                            if col != "municipio_code"
                        }

                        try:
                            fractal_engine.execute(
                                update_sql,
                                {**params, "municipio_code": row["municipio_code"]},
                            )
                            cont_update += 1
                        except SQLAlchemyError as e:
                            logger.info(
                                f"Error updating row with municipio_code {row['municipio_code']}: {e}"
                            )
                            trans.rollback()
                            continue

                    else:
                        # Inserir a nova linha
                        insert_sql = text(
                            """
                            INSERT INTO city_reference ({})
                            VALUES ({})
                        """.format(
                                ", ".join(df_divergencias.columns),
                                ", ".join(f":{col}" for col in df_divergencias.columns),
                            )
                        )
                        params = {col: row[col] for col in df_divergencias.columns}

                        try:
                            fractal_engine.execute(insert_sql, params)
                            cont_insert += 1
                        except SQLAlchemyError as e:
                            logger.info(
                                f"Error inserting row with municipio_code {row['municipio_code']}: {e}"
                            )
                            trans.rollback()
                            continue

                # Confirmar a transação
                trans.commit()
        logger.info(f"Total de linhas inseridas: {cont_insert}")
        logger.info(f"Total de linhas atualizadas: {cont_update}")
        sys.exit(0)
