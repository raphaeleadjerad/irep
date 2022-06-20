import os
import s3fs
import pandas as pd
from collections import ChainMap
from elasticsearch import Elasticsearch

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


# ELASTIC

HOST = 'elasticsearch-master.projet-ssplab'


def elastic():
    """Connection avec Elastic sur le data lab"""
    es = Elasticsearch([{'host': HOST, 'port': 9200, 'scheme': 'http'}], http_compress=True, request_timeout=200)
    return es

es = elastic()

