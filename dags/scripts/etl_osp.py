import logging
import sys

import geopandas as gpd
import pandas as pd
from shapely import wkt
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .utils.db_connection import DBConnection
from .utils.outside_the_border import Otb


def main(**kwargs):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    id_city = kwargs.get('id_city', None)
    logger.info(f"Buscando dados da cidade com ID: {id_city}")
    cities = pd.DataFrame.from_records(kwargs.get('cities', None))
      
    conn = DBConnection()
    otb = Otb()
    avalon_engine = conn.getEngine('diana_trino')
    fractal_engine = conn.getEngine('fractal_db')

    def expurgo(id, tableName, engine):
        logger.info(f"Iniciando processo de expurgo.")
        sagre_purge_query = text(f"""DELETE FROM {tableName} WHERE id_city = {id};""")
        try:
            if conn.is_connection_active(engine):
                result = engine.execute(sagre_purge_query)
                logger.info(f"Número de linhas expurgadas: {result.rowcount}")

        except Exception as e:
            logger.error(f"Erro ao executar EXPURGO: {e}")
            sys.exit(1)
            
    def convertGeoDataFrame(df):
        try:
            if 'geometry' in df.columns:
                df['geometry'] = df['geometry'].apply(wkt.loads)
            else:
                logger.error("A coluna 'geometry' está faltando no DataFrame.")
                sys.exit(1)
            
            df_result = gpd.GeoDataFrame(df, geometry='geometry') # type: ignore
            df_result.set_crs(epsg=4326, inplace=True)
            
            return df_result
        except Exception as e:
            logger.error(f"Error ao criar o GeoDataFrame: {e}")
            sys.exit(1)

    def save(tableName, df, row):
        print(f"Realizando a carga dos dados no banco de dados.")  
        if not df.empty:
            df = df.set_geometry('geometry')
            df.set_crs(epsg=4326, inplace=True)
            df['id_city'] = id_city 
            df.to_postgis(tableName, con=fractal_engine, if_exists='append', index=False)
            num_rows = len(df)
            logger.info(f"Processamento finalizado da cidade: {row['city']}.\nFoi realizada a carga de {num_rows} linhas.")
        else:
            logger.info(f"Processamento finalizado da cidade: {row['city']}.\nA cidade não possui dados cadastrados.")


    bancoDadosOsp = []
    city_uf_list = []
    for _, row in cities.iterrows():
        city_uf_list.append(f"{row['city']} - {row['uf']}")
        
        query = f"""
            SELECT DISTINCT 
                CASE 
                    WHEN banco_dados_osp IS NULL THEN 'TLF_BRASIL' 
                    ELSE banco_dados_osp 
                END AS banco_dados_osp
            FROM public.city_reference cr
            WHERE cr.municipio_code = '{row['code']}'
        """
        try:
            bancoDadosOsp.append(pd.read_sql(query, con=fractal_engine).iloc[0, 0])
        except Exception as e:
            logger.error(f"Erro ao executar a consulta SQL: {e}")
            sys.exit(1)
            
            
    # bancoDadosOsp_str = ", ".join(f"'{item}'" for item in bancoDadosOsp)
    result = ", ".join(city_uf_list)
    
    logger.info(f"Buscando dados da(s) cidade(s): {result}")
    
    for valor in bancoDadosOsp:
        if valor != "TLF_BRASIL":
            query = f"""
                SELECT
                    tipo_instalacao_cabo_tipo_instalacao as rede,
                    'VIVO1' as malha,
                    CASE
                        WHEN cabo_logico_funcao = '1 - Tronco' THEN 'BACKHAUL'
                        WHEN cabo_logico_funcao = '19 - Anel' THEN 'B2B'
                        ELSE cabo_logico_funcao
                    END as tipo_rede,
                    'OPTICO' as tecnologia,
                    lance_cabo_optico_situacao as lance_de_c,
                    lance_cabo_optico_id as lance_de_1,
                    tipo_lance_cabo_optico_material as tipo_de_ca,
                    tipo_lance_cabo_optico_id as tipo_de_1,
                    lance_cabo_optico_cd as lance_de_2,
                    lance_cabo_optico_rota as lance_de_3,
                    REPLACE(banco_dados_osp, 'TLF_BR_', '') as layer,
                    contagem_id as contagem_i,
                    contagem_cabo_logico as contagem_c,
                    cabo_logico_funcao as funcao,
                    cabo_logico_site_origem_cd as site_a,
                    cabo_logico_site_destino_cd as site_b,
                    cabo_logico_cd as etiqueta,
                    cabo_logico_lateral_lateral as lateral_,
                    cabo_logico_local_origem_cnl as cnl_cod_a,
                    cabo_logico_local_origem_nm as cnl_nome_a,
                    cabo_logico_local_origem_sigla as cnl_sigla,
                    cabo_logico_local_destino_cnl as cnl_cod_b,
                    cabo_logico_local_destino_nm as cnl_nome_b,
                    cabo_logico_local_destino_sigla	as cnl_sigl_1,
                    name as name,
                    geometry
                FROM mysql_avalon.raw_outside_plant.osp_lance_cabo_optico 
                WHERE cabo_logico_funcao NOT IN ('11 - Alimentador FTTX','18 - Cabo de Equipamento','6 - Privativo','3 - Distribuidor')
                AND banco_dados_osp IN ({str(valor)})
            """
            geometry_ARQ = pd.read_sql(query, con=avalon_engine)

            logger.info("Convertendo DataFrame para GeoDataFrame")
            df = convertGeoDataFrame(geometry_ARQ)
            
            logger.info("Buscando Polygon")
            polygon = otb.getPolygon(id_city)

            logger.info("Filtrando os dados que intersectam com o polígono do município")
            df_result =  df[df.intersects(polygon)]
            
            expurgo(id_city, 'osp_sp', fractal_engine)        
            
            save('osp_sp', df_result, row)
            
        else:
            polygon = otb.getPolygon(id_city)
            polygon_wkt = wkt.dumps(polygon)
            sql_query = f"""
            SELECT 
                UPPER(
                    SUBSTRING(tipo_instalacao_cabo_tipo_instalacao FROM POSITION('- ' IN tipo_instalacao_cabo_tipo_instalacao) + 2)
                ) || lance_cabo_optico_id as ID_OSP, 
                lance_cabo_optico_situacao as SITUACAO, 
                lance_cabo_optico_cd as CODIGO, 
                lance_cabo_optico_rota as ROTA, 
                tipo_lance_cabo_optico_material as TIPO_CABO, 
                tipo_lance_cabo_optico_qt as QTD_FIBRA, 
                cabo_logico_id as id_cabo_logico_optico, 
                cabo_logico_funcao as FUNCAO, 
                cabo_logico_cd as tronco, 
                cabo_logico_site_origem_id as ID_SITEA, 
                cabo_logico_site_destino_id as ID_SITEB, 
                cabo_logico_site_origem_cd as SITEA, 
                cabo_logico_site_destino_cd as SITEB, 
                geometry 
            FROM osp_lance_cabo_optico_tlf_brasil
            WHERE cabo_logico_funcao NOT IN ('11 - Alimentador FTTX','18 - Cabo de Equipamento','6 - Privativo','3 - Distribuidor')
            AND ST_Within(ST_SetSRID(geometry, 4326), ST_GeomFromText('{polygon_wkt}', 4326));
            """

            # Executar a query e carregar os resultados em um GeoDataFrame
            df_result = gpd.read_postgis(sql_query, fractal_engine, geom_col='geometry')
            
            expurgo(id_city, 'osp', fractal_engine)        
            
            save('osp', df_result, row)

        


    sys.exit(0)