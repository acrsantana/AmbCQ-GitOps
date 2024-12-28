import os
from functools import lru_cache
from tempfile import NamedTemporaryFile

import geobr
import geobr.utils
import geopandas as gpd
import pandas as pd


@lru_cache(maxsize=1240)
def __load_gpkg(url):

    try:
        content = geobr.utils.url_solver(url).content

    except Exception as e:

        raise Exception(
            "Some internal url is broken."
            "Please report to https://github.com/ipeaGIT/geobr/issues"
        )

    with NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp_file:
        tmp_file.write(content)
        tmp_file.flush()
        gdf = gpd.read_file(tmp_file.name)

    os.remove(tmp_file.name)

    return gdf


def __download_gpkg(metadata):

    urls = metadata["download_path"].tolist()

    gpkgs = [__load_gpkg(url) for url in urls]

    df = gpd.GeoDataFrame(pd.concat(gpkgs, ignore_index=True))

    df = geobr.utils.enforce_types(df)

    return df


def read_municipality_custom(
    code_muni="all", year=2010, simplified=True, verbose=False
):

    metadata = geobr.utils.select_metadata(
        "municipality", year=year, simplified=simplified
    )

    if year < 1992:

        return __download_gpkg(metadata)

    if code_muni == "all":

        if verbose:
            print("Loading data for the whole country. This might take a few minutes.")

        return __download_gpkg(metadata)

    metadata = metadata[
        metadata[["code", "code_abbrev"]].apply(
            lambda x: str(code_muni)[:2] in str(x["code"])
            or str(code_muni)[:2]  # if number e.g. 12
            in str(x["code_abbrev"]),  # if UF e.g. RO
            1,
        )
    ]

    if not len(metadata):
        raise Exception("Invalid Value to argument code_muni.")

    gdf = __download_gpkg(metadata)

    if len(str(code_muni)) == 2:
        return gdf

    elif code_muni in gdf["code_muni"].tolist():
        return gdf.query(f"code_muni == {code_muni}")

    else:
        raise Exception("Invalid Value to argument code_muni.")
    return gdf
