import os
import s3fs
import pandas as pd
from collections import ChainMap
from elasticsearch import Elasticsearch
import geopandas as gpd
import rapidfuzz
import numpy as np


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

def pipeline_request(df_temp, request_template, cols):
    header = '{"index" : "sirus_2020"}'
    multiple_requetes = ""
    n_etab = df_temp.shape[0] 

    for index, row in df_temp.iloc[0:n_etab][cols].iterrows():
    
        multiple_requetes+= header
        multiple_requetes+= '\n'
        multiple_requetes+= request_template.format_map(row).replace("\n","")
        multiple_requetes+= '\n'
    
    res = es.msearch(body = multiple_requetes)

    df_temp["siret_elastic"] = [res['responses'][i]['hits']['hits'][0]["_source"]["siret_id"] if \
        res['responses'][i]['hits']['hits'] else np.NaN for i in range(df_temp.shape[0]) ]
    df_temp["match"] = (df_temp["numero_siret_true"].astype(str) == df_temp["siret_elastic"].astype(str))

    df_temp["nom_etab_elastic"] = [res['responses'][i]['hits']['hits'][0]["_source"]["denom"] if \
        res['responses'][i]['hits']['hits'] else np.NaN for i in range(df_temp.shape[0]) ]

    df_temp['nom_etablissement'] = df_temp['nom_etablissement'].astype(str) 
    df_temp["textual_distance"] = pd.concat(
          [df_temp.apply(lambda x: rapidfuzz.fuzz.partial_ratio(
              x["nom_etab_elastic"],
              x[y]),
              axis=1) for y in ["nom_etablissement"]],
          axis=1
      )
    print(f"We found correct SIRET in {df_temp['match'].mean():.2%} of cases")
    return df_temp

