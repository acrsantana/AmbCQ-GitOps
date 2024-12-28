import logging
import sys

import pandas as pd
from sqlalchemy import text

from .utils.db_connection import DBConnection


def main(**kwargs):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    id_city = kwargs.get("id_city", None)

    conn = DBConnection()

    diana_engine = conn.getEngine("diana_trino")
    fractal_engine = conn.getEngine("fractal_db")

    logger.info(f"Buscando dados da cidade com ID: {id_city}")
    query = f"""
        SELECT upper(cr.municipio_nm) as city, uf_sigla as uf
        FROM public.city_reference cr 
        WHERE cr.municipio_code IN (
            SELECT unnest(c.content_cities) 
            FROM city c 
            WHERE c.id = {id_city}
        ) ORDER BY uf_sigla, city;
    """
    try:
        cities = pd.read_sql(query, con=fractal_engine)
    except Exception as e:
        logger.error(f"Erro ao executar a consulta SQL: {e}")
        sys.exit(1)

    logger.info(f"Iniciando processo de expurgo.")
    sagre_purge_query = text(
        f"""DELETE FROM structuring_sites WHERE id_city = {id_city};"""
    )
    try:
        if conn.is_connection_active(fractal_engine):
            result = fractal_engine.execute(sagre_purge_query)
            logger.info(f"Total de linhas expurgadas: {result.rowcount}")

    except Exception as e:
        logger.error(f"Erro ao executar DELETE: {e}")
        sys.exit(1)

    for _, row in cities.iterrows():
        logger.info(f"Iniciando processamento da cidade: {row['city']}, {row['uf']}")
        logger.info(f"Bucando sites estruturantes da cidade.")
        query_sites = """
                SELECT DISTINCT
                    sgnc.sigla_site as SIG_SITE,
                    sgnc.situacao as STATUS_SITE,
                    sgnc.des_tipo_restricao as RESTRICAO,
                    rdc.sts_site_vip as SITE_VIP, 
                    ARRAY_AGG(DISTINCT se.siglaet) AS EQUIP_METRO_ATUAL,
                    ARRAY_AGG(DISTINCT 
                        CASE 
                            WHEN UPPER(se.siglaet) LIKE '%-HL3-%' THEN '-hl3-'
                            WHEN UPPER(se.siglaet) LIKE '%-HL4-%' THEN '-hl4-'
                            WHEN UPPER(se.siglaet) LIKE '%-HL5D-%' THEN '-hl5d-'
                            WHEN UPPER(se.siglaet) LIKE '%-HL5G-%' THEN '-hl5g-'
                        END
                    ) AS EQUIP_METRO,
                    ARRAY_AGG(DISTINCT se.status) AS STATUS_METRO,
                    CASE 
                        WHEN COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'I-%' THEN 1 END) > 0
                            AND COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'M-%' THEN 1 END) > 0 THEN 'Ambas'
                        WHEN COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'I-%' THEN 1 END) > 0 THEN 'Fusion'
                        WHEN COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'M-%' THEN 1 END) > 0 THEN 'Legada'
                        ELSE 'Sem Metro'
                    END AS TECNOLOGIA_METRO,
                    ARRAY_AGG(DISTINCT rdc.sig_tecnologia) AS TECNOLOGIA_RF,
                    ARRAY_AGG(DISTINCT se.tipoequip) AS TIPOS_EQUIP_TX,
                    sgnc.num_latitude_decimal_raw as NUM_LATITUDE_DECIMAL, 
                    sgnc.num_longitude_decimal_raw as NUM_LONGITUDE_DECIMAL,
                    ARRAY_AGG(DISTINCT
                    CASE 
                        WHEN sgnc.ransharing = 'SIM'
                            OR sgnc.tipo_acomodacao_ev = 'Rooftop'
                            OR sgnc.des_tipo_restricao = 'AREA DE RISCO' 
                        THEN False
                        ELSE True
                    END ) AS hub_hl3,
                    ARRAY_AGG(DISTINCT
                    CASE 
                        WHEN sgnc.ransharing = 'SIM'
                            OR sgnc.tipo_acomodacao_ev = 'Rooftop'
                            OR sgnc.des_tipo_restricao = 'AREA DE RISCO' 
                        THEN False
                        ELSE True
                    END ) AS hub_hl4,
                    ARRAY_AGG(DISTINCT
                    CASE 
                        WHEN sgnc.ransharing = 'SIM'
                            OR sgnc.tipo_acomodacao_ev = 'Rooftop'
                            OR sgnc.des_tipo_restricao = 'AREA DE RISCO' 
                        THEN False
                        ELSE True
                    END ) AS hub_hl5d
                FROM raw_science.site_gsci_new_cadastros sgnc 
                LEFT JOIN raw_science.rf_dados_completa rdc ON(sgnc.uf_site = rdc.uf_site)
                LEFT JOIN raw_smtx.smtx_equipamento se ON(sgnc.uf_site = se.uf_site) 
                WHERE sgnc.uf = ?
                AND sgnc.nom_municipio = ?
                AND sgnc.num_latitude_decimal_raw IS NOT NULL
                AND sgnc.num_longitude_decimal_raw IS NOT NULL
                AND sgnc.situacao NOT IN ('CANCELADO', 'DISTRATADO', 'EXCLUÍDO')
                AND se.status NOT IN ('Desativado', 'À Desativar')
                AND (sgnc.flg_ransharing = 'NAO' OR sgnc.flg_ransharing IS NULL)
                AND (sgnc.flg_mou = 'NAO' OR sgnc.flg_mou IS NULL)
                GROUP BY 
                    sgnc.sigla_site,
                    sgnc.situacao,
                    sgnc.des_tipo_restricao,
                    rdc.sts_site_vip,
                    sgnc.num_latitude_decimal_raw,
                    sgnc.num_longitude_decimal_raw
                ORDER BY sgnc.sigla_site
            """

        df_sites = pd.read_sql(
            query_sites, con=diana_engine, params=(row["uf"], row["city"])
        )

        df_sites["hub_hl3"] = df_sites["hub_hl3"].apply(lambda x: x[0]).astype(bool)
        df_sites["hub_hl4"] = df_sites["hub_hl4"].apply(lambda x: x[0]).astype(bool)
        df_sites["hub_hl5d"] = df_sites["hub_hl5d"].apply(lambda x: x[0]).astype(bool)

        logger.info(f"Total de sites encontrados: {len(df_sites)}")

        elementos_hl4 = [
            item
            for item in df_sites.explode("EQUIP_METRO_ATUAL")[
                "EQUIP_METRO_ATUAL"
            ].tolist()
            if "-hl4-" in item
        ]
        elementos_hl4_str = "('" + "', '".join(elementos_hl4) + "')"

        df_sites_hl3 = pd.DataFrame()

        if not elementos_hl4:
            logger.info("A lista elementos_hl4 não está vazia.")

            logger.info(f"Buscano elementos HL3 relacionados aos HL4.")
            query_rsnac = f"""
                SELECT DISTINCT listHL3 
                FROM (
                    SELECT hosta AS listHL3 
                    FROM raw_rsnac.conexoescompativeis_all
                    WHERE hostb IN {elementos_hl4_str}
                    AND hosta LIKE '%-hl3-%'
                    
                    UNION ALL
                    
                    SELECT hostb AS listHL3 
                    FROM raw_rsnac.conexoescompativeis_all
                    WHERE hosta IN {elementos_hl4_str}
                    AND hostb LIKE '%-hl3-%'
                ) AS list
            """

            # Execute a consulta passando o parâmetro como uma tupla
            df_rsnac = pd.read_sql(query_rsnac, con=diana_engine)

            if not df_rsnac.empty:

                elementos_hl3 = df_rsnac["listHL3"].tolist()
                elementos_hl3_str = "('" + "', '".join(elementos_hl3) + "')"

                query_sites_hl3 = f"""
                        SELECT DISTINCT
                            sgnc.sigla_site as SIG_SITE,
                            sgnc.situacao as STATUS_SITE,
                            sgnc.des_tipo_restricao as RESTRICAO,
                            rdc.sts_site_vip as SITE_VIP, 
                            ARRAY_AGG(DISTINCT se.siglaet) AS EQUIP_METRO_ATUAL,
                            ARRAY_AGG(DISTINCT 
                                CASE 
                                    WHEN UPPER(se.siglaet) LIKE '%-HL3-%' THEN '-hl3-'
                                    WHEN UPPER(se.siglaet) LIKE '%-HL4-%' THEN '-hl4-'
                                    WHEN UPPER(se.siglaet) LIKE '%-HL5D-%' THEN '-hl5d-'
                                    WHEN UPPER(se.siglaet) LIKE '%-HL5G-%' THEN '-hl5g-'
                                END
                            ) AS EQUIP_METRO,
                            ARRAY_AGG(DISTINCT se.status) AS STATUS_METRO,
                            CASE 
                                WHEN COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'I-%' THEN 1 END) > 0
                                    AND COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'M-%' THEN 1 END) > 0 THEN 'Ambos'
                                WHEN COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'I-%' THEN 1 END) > 0 THEN 'Fusion'
                                WHEN COUNT(DISTINCT CASE WHEN UPPER(se.siglaet) LIKE 'M-%' THEN 1 END) > 0 THEN 'Legado'
                                ELSE 'Sem Metro'
                            END AS TECNOLOGIA_METRO,
                            ARRAY_AGG(DISTINCT rdc.sig_tecnologia) AS TECNOLOGIA_RF,
                            ARRAY_AGG(DISTINCT se.tipoequip) AS TIPOS_EQUIP_TX,
                            sgnc.num_latitude_decimal_raw as NUM_LATITUDE_DECIMAL, 
                            sgnc.num_longitude_decimal_raw as NUM_LONGITUDE_DECIMAL,
                            ARRAY_AGG(DISTINCT
                            CASE 
                                WHEN sgnc.ransharing = 'SIM'
                                    OR sgnc.tipo_acomodacao_ev = 'Rooftop'
                                    OR sgnc.des_tipo_restricao = 'AREA DE RISCO' 
                                THEN False
                                ELSE True
                            END ) AS hub_hl3,
                            ARRAY_AGG(DISTINCT
                            CASE 
                                WHEN sgnc.ransharing = 'SIM'
                                    OR sgnc.tipo_acomodacao_ev = 'Rooftop'
                                    OR sgnc.des_tipo_restricao = 'AREA DE RISCO' 
                                THEN False
                                ELSE True
                            END ) AS hub_hl4,
                            ARRAY_AGG(DISTINCT
                            CASE 
                                WHEN sgnc.ransharing = 'SIM'
                                    OR sgnc.tipo_acomodacao_ev = 'Rooftop'
                                    OR sgnc.des_tipo_restricao = 'AREA DE RISCO' 
                                THEN False
                                ELSE True
                            END ) AS hub_hl5d
                        FROM raw_science.site_gsci_new_cadastros sgnc 
                        LEFT JOIN raw_science.rf_dados_completa rdc ON(sgnc.uf_site = rdc.uf_site)
                        LEFT JOIN raw_smtx.smtx_equipamento se ON(sgnc.uf_site = se.uf_site) -- and se.status = 'Ativado')
                        WHERE se.siglaet IN {elementos_hl3_str}
                        AND sgnc.situacao NOT IN ('CANCELADO', 'DISTRATADO', 'EXCLUÍDO')
                        AND se.status NOT IN ('Desativado', 'À Desativar')
                        AND (sgnc.flg_ransharing = 'NAO' OR sgnc.flg_ransharing IS NULL)
                        AND (sgnc.flg_mou = 'NAO' OR sgnc.flg_mou IS NULL)
                        AND sgnc.num_latitude_decimal_raw IS NOT NULL
                        AND sgnc.num_longitude_decimal_raw IS NOT NULL
                        GROUP BY 
                            sgnc.sigla_site,
                            sgnc.situacao,
                            sgnc.des_tipo_restricao,
                            rdc.sts_site_vip,
                            sgnc.num_latitude_decimal_raw,
                            sgnc.num_longitude_decimal_raw
                        ORDER BY sgnc.sigla_site
                    """

                df_sites_hl3 = pd.read_sql(query_sites_hl3, con=diana_engine)

                logger.info(f"Total de sites HL3 encontrados: {len(df_sites_hl3)}")

                df_sites_hl3["hub_hl3"] = (
                    df_sites_hl3["hub_hl3"].apply(lambda x: x[0]).astype(bool)
                )
                df_sites_hl3["hub_hl4"] = (
                    df_sites_hl3["hub_hl4"].apply(lambda x: x[0]).astype(bool)
                )
                df_sites_hl3["hub_hl5d"] = (
                    df_sites_hl3["hub_hl5d"].apply(lambda x: x[0]).astype(bool)
                )

        if not df_sites_hl3.empty:
            logger.info(f"Concatenando elementos hl3 a nossa massa de dados.")
            df_estruturante = pd.concat([df_sites, df_sites_hl3], axis=0)
            df_estruturante.reset_index(inplace=True, drop=True)

            logger.info(f"Total de sites encontrados: {len(df_estruturante)}")
            logger.info(f"Realizando a carga dos dados no banco de dados.")
        else:
            df_estruturante = df_sites
            logger.info(f"Total de sites encontrados: {len(df_estruturante)}")
            logger.info(f"Realizando a carga dos dados no banco de dados.")

        if not df_estruturante.empty:
            df_estruturante.columns = df_estruturante.columns.str.lower()
            df_estruturante["id_city"] = id_city
            df_estruturante.to_sql(
                "structuring_sites", con=fractal_engine, if_exists="append", index=False
            )
            num_rows = len(df_estruturante)
            logger.info(
                f"Processamento finalizado da cidade: {row['city']}.\nFoi realizada a carga de {num_rows} linhas."
            )
        else:
            logger.info(
                f"Processamento finalizado da cidade: {row['city']}.\nA cidade não possui dados cadastrados."
            )

    sys.exit(0)
