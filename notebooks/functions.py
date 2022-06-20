import os
import s3fs
import pandas as pd
from collections import ChainMap
from elasticsearch import Elasticsearch
import geopandas as gpd


# Create filesystem object
S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})
BUCKET = "radjerad"
PATH_MINIO = "diffusion/irep"

list_bases = fs.ls(f"{BUCKET}/{PATH_MINIO}")
list_bases = [fl for fl in list_bases if fl.endswith(".csv")]

def wrap_read_s3(file_path):
    with fs.open(file_path, mode="rb") as file_in:
        df = pd.read_csv(file_in, sep=";")
    return {file_path.rsplit("/")[-1].replace(".csv", "") : df}

def read_all_raw(list_bases):
    list_dicts = [wrap_read_s3(fl) for fl in list_bases]
    list_dicts = dict(ChainMap(*list_dicts))
    return list_dicts

def transform_wgs84(df, epsg):
    etab = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(
            df['coordonnees_x'],
            df['coordonnees_y']
        ),
        crs = epsg)
    etab = etab.to_crs(4326)
    etab['x'] = etab['geometry'].x 
    etab['y'] = etab['geometry'].y
    etab = pd.DataFrame(etab)
    return etab


# ELASTIC

HOST = 'elasticsearch-master.projet-ssplab'


def elastic():
    """Connection avec Elastic sur le data lab"""
    es = Elasticsearch([{'host': HOST, 'port': 9200, 'scheme': 'http'}], http_compress=True, request_timeout=200)
    return es

es = elastic()

def get_product_echo(echo):
    if echo:
        return echo[0]['_source']
    else:
        return None


def clean_data_etab(df):
    etab = df
    etab_not_null = etab.dropna(subset = ['code_epsg'])
    etab_null = etab.loc[etab['code_epsg'].isnull()]
    gb = etab_not_null.groupby("code_epsg")
    gb = [gb.get_group(x) for x in gb.groups]
    temp = [
        transform_wgs84(
            gb[idx],
            gb[idx]['code_epsg'].iloc[0]
        ) for idx in range(len(gb)) 
    ]
    temp = pd.concat(
        temp
    )
    etab = pd.concat(
        [temp, etab_null]
    )
    etab = etab.rename({'numero_siret': "numero_siret_true"}, axis = 1)
    etab["code_apet"] = etab["code_ape"].str[:4]
    return etab
