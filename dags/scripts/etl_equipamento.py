import logging
import sys

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .utils.db_connection import DBConnection


def main(**kwargs):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    id_city = kwargs.get('id_city', None)

    db_conn = DBConnection()
    diana_engine = db_conn.getEngine('diana_trino')
    fractal_engine = db_conn.getEngine('fractal_db')


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


    for _, row in cities.iterrows():
        logger.info(f"Iniciando processamento da cidade: {row['city']}, {row['uf']}")
        query_equip_sites = """
                SELECT DISTINCT
                    se.siglaet AS EQUIP_METRO_ATUAL,
                    se.fabricanteequip AS FABRICANTES_EQUIP,
                    se.modeloequip AS MODELOS_EQUIP,
                    CASE 
                        WHEN UPPER(se.siglaet) LIKE '%-HL3-%' THEN 'hl3'
                        WHEN UPPER(se.siglaet) LIKE '%-HL4-%' THEN 'hl4'
                        WHEN UPPER(se.siglaet) LIKE '%-HL5D-%' THEN 'hl5d'
                        WHEN UPPER(se.siglaet) LIKE '%-HL5G-%' THEN 'hl5g'
                    END AS TIPOS_EQUIP
                FROM raw_science.site_gsci_new_cadastros sgnc 
                LEFT JOIN raw_science.rf_dados_completa rdc ON (sgnc.uf_site = rdc.uf_site)
                LEFT JOIN raw_smtx.smtx_equipamento se ON (sgnc.uf_site = se.uf_site)
                WHERE sgnc.uf = ?
                AND sgnc.nom_municipio = ?
                AND sgnc.situacao NOT IN ('CANCELADO', 'DISTRATADO', 'EXCLUÍDO')
                AND se.status NOT IN ('Desativado', 'À Desativar')
                AND (sgnc.flg_ransharing = 'NAO' OR sgnc.flg_ransharing IS NULL)
                AND (sgnc.flg_mou = 'NAO' OR sgnc.flg_mou IS NULL)
                AND (
                    UPPER(se.siglaet) LIKE '%-HL3-%' OR
                    UPPER(se.siglaet) LIKE '%-HL4-%' OR
                    UPPER(se.siglaet) LIKE '%-HL5D-%' OR
                    UPPER(se.siglaet) LIKE '%-HL5G-%'
                )
                GROUP BY 
                    se.siglaet,
                    se.fabricanteequip,
                    se.modeloequip,
                    se.tipoequip
                ORDER BY se.siglaet
            """

        df_equip_sites = pd.read_sql(query_equip_sites, con=diana_engine, params=(row['uf'], row['city']))

        # Extraindo valores distintos do DataFrame
        df_equip = df_equip_sites[['FABRICANTES_EQUIP', 'MODELOS_EQUIP', 'TIPOS_EQUIP']].drop_duplicates()
        
        df_equip.columns = df_equip.columns.str.lower()
        
        df_equip = df_equip.fillna('')
        
        with fractal_engine.connect() as conn: # type: ignore
            cont_insert = 0
            with conn.begin() as trans:
                for _, row in df_equip.iterrows():
                    check_sql = text("""
                        SELECT COUNT(*) 
                        FROM equipamento_referencia 
                        WHERE fabricantes_equip = :fabricantes_equip
                        AND modelos_equip = :modelos_equip
                        AND tipos_equip = :tipos_equip
                    """)
                    result = conn.execute(check_sql, {'fabricantes_equip': row['fabricantes_equip'], 'modelos_equip': row['modelos_equip'], 'tipos_equip': row['tipos_equip']})
                    exists = result.scalar() > 0
                    
                    if exists:
                        logger.info(f"Equipamento [{row['fabricantes_equip']}, {row['modelos_equip']} - {row['tipos_equip']}] já cadastrado!")
                    else:
                        # Inserir a nova linha
                        insert_sql = text("""
                            INSERT INTO equipamento_referencia ({})
                            VALUES ({})
                        """.format(
                            ', '.join(df_equip.columns),
                            ', '.join(f":{col}" for col in df_equip.columns)
                        ))
                        params = {col: row[col] for col in df_equip.columns}
                        
                        try:
                            conn.execute(insert_sql, params)
                            cont_insert += 1
                        except SQLAlchemyError as e:
                            logger.error(f"Error inserting row with municipio_code {row['municipio_code']}: {e}")
                            trans.rollback()
                            continue

                # Confirmar a transação
                trans.commit()
        logger.info(f"Total de linhas inseridas: {cont_insert}")  
        
        # Recuperando as chaves primárias atualizadas da tabela 'equipamento_referencia'
        query_referencia = """
            SELECT id, FABRICANTES_EQUIP, MODELOS_EQUIP, TIPOS_EQUIP 
            FROM equipamento_referencia
        """
        df_referencia = pd.read_sql(query_referencia, con=fractal_engine)
        
        sagre_purge_query = text(f"""DELETE FROM equipamento_site WHERE id_city = {id_city};""")
        try:
            result = fractal_engine.execute(sagre_purge_query)
            print(f"Número de linhas afetadas: {result.rowcount}")

        except Exception as e:
            print(f"Erro ao executar DELETE: {e}")
            sys.exit(1)
        
        df_equip_sites.columns = df_equip_sites.columns.str.lower()

        # Fazendo o merge para obter a chave primária correspondente
        df_merge = pd.merge(df_equip_sites, df_referencia, on=['fabricantes_equip', 'modelos_equip', 'tipos_equip'])

        # Selecionando as colunas necessárias para a tabela 'equipamento_site'
        df_equipamento_site = df_merge[['equip_metro_atual', 'id']].rename(columns={'id': 'id_equipamento_referencia'})
        df_equipamento_site['id_city'] = id_city 

        # Inserindo os relacionamentos na tabela 'equipamento_site'
        df_equipamento_site.to_sql('equipamento_site', con=fractal_engine, if_exists='append', index=False)

    sys.exit(0)


