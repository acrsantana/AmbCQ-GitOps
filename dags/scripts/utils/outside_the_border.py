import logging
import sys

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
from shapely.ops import unary_union

from .db_connection import DBConnection
from .geobr_custom import read_municipality_custom

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_conn = DBConnection()


class Otb:

    def __init__(self):
        self.fractal_engine = db_conn.getEngine("fractal_db")

    def __getCitys(self, id_city):
        logger.info(f"Buscando dados da cidade com ID: {id_city}")
        query = f"""
            SELECT municipio_code as code, regexp_replace(unaccent(upper(cr.municipio_nm)), '[^A-Z0-9_]', '_', 'g') as city, uf_sigla as uf, banco_dados_osp
            FROM public.city_reference cr 
            WHERE cr.municipio_code IN (
                SELECT unnest(c.content_cities) 
                FROM city c 
                WHERE c.id = {id_city}
            ) ORDER BY uf_sigla, city;
        """
        try:
            dfCitys = pd.read_sql(query, con=self.fractal_engine)
            return dfCitys
        except Exception as e:
            logger.error(f"Erro ao executar a consulta SQL: {e}")
            sys.exit(1)

    def __getStructuringSites(self, id_city):
        logger.info(f"Buscando dados dos SITES da cidade com ID: {id_city}")
        df_structuring_sites = None
        try:
            query = f"""
                SELECT pss.*, array_remove(pss.equip_metro, NULL) AS equip_metro_notnull 
                FROM public.structuring_sites pss 
                WHERE pss.id_city = {id_city}
            """
            df_structuring_sites = pd.read_sql(query, con=self.fractal_engine)
            return df_structuring_sites
        except Exception as e:
            logger.error(f"Erro ao executar a consulta so SAGRE: {e}")
            sys.exit(1)

    def __getOutrosMunicipios(self, id_city):
        dfCitys = self.__getCitys(id_city)
        df_structuring_sites = self.__getStructuringSites(id_city)

        polygon_original = []

        for _, city in dfCitys.iterrows():
            if not pd.isnull(city["code"]) and city["code"] != "":
                gdf = read_municipality_custom(code_muni=int(city["code"]), year=2022)
                polygon = gdf.iloc[0].geometry
                polygon_original.append(polygon)
            else:
                print(f"Erro ao carregar o polígono do município: {city['city']}")
                sys.exit(1)

        polygon_original = unary_union(polygon_original)

        outside_sites = []
        for _, site in df_structuring_sites.iterrows():
            latitude = site["num_latitude_decimal"]
            longitude = site["num_longitude_decimal"]
            point = Point(longitude, latitude)
            if not polygon_original.contains(point):
                outside_sites.append((site["sig_site"], latitude, longitude))

        municipios = read_municipality_custom(year=2020)

        pontos = gpd.GeoDataFrame(
            outside_sites, columns=["ID", "Latitude", "Longitude"]
        )
        pontos["geometry"] = pontos.apply(
            lambda row: Point(np.array([row["Longitude"], row["Latitude"]])), axis=1
        )
        pontos = pontos.set_geometry("geometry")
        municipios = municipios.to_crs(epsg=4326)
        outros_municipios = gpd.sjoin(
            pontos, municipios, how="left", predicate="within"
        )

        if outros_municipios["code_muni"].isnull().any():
            outros_municipios = outros_municipios.dropna(subset=["code_muni"])

        outros_municipios["code_muni"] = outros_municipios["code_muni"].astype(int)

        return outros_municipios, dfCitys, polygon_original

    def getOutOfBorderSites(self, id_city):
        outros_municipios, dfCitys, _ = self.__getOutrosMunicipios(id_city)

        new_rows = []
        for _, row in outros_municipios.iterrows():
            new_row = pd.DataFrame(
                [
                    {
                        "code": row["code_muni"],
                        "city": row["name_muni"],
                        "uf": row["abbrev_state"],
                        "banco_dados_osp": None,
                    }
                ]
            )
            new_rows.append(new_row)

        # Concatene todas as novas linhas ao DataFrame dfCitys
        if new_rows:
            dfCitys = pd.concat([dfCitys] + new_rows, ignore_index=True)
            dfCitys = dfCitys.drop_duplicates()

        return dfCitys

    def getPolygon(self, id_city):
        pontos_com_municipio, _, polygon_original = self.__getOutrosMunicipios(id_city)

        outros_polygon_list = []
        polygon_list = []

        pontos_com_municipio = pontos_com_municipio.drop_duplicates(
            subset=["code_muni"]
        )

        for _, row in pontos_com_municipio.iterrows():
            gdf = read_municipality_custom(code_muni=row["code_muni"], year=2022)
            polygon = gdf.iloc[0].geometry
            outros_polygon_list.append(polygon)

        polygon_list.extend([polygon_original])
        polygon_list.extend(outros_polygon_list)

        buffered_polygons = [polygon.buffer(0.001) for polygon in polygon_list]
        combined_polygon = unary_union(buffered_polygons)
        combined_polygon = combined_polygon.buffer(-0.001)

        return combined_polygon
