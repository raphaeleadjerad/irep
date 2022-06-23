import functions as fc
from elasticsearch import Elasticsearch
HOST = 'elasticsearch-master'



import s3fs

fs = s3fs.S3FileSystem(
  client_kwargs={'endpoint_url': 'https://minio.lab.sspcloud.fr'})

list_files = fs.ls("radjerad/diffusion/sirene")
fs.download('radjerad/diffusion/sirene/StockEtablissement_utf8.zip','siret.zip')

import zipfile
with zipfile.ZipFile('siret.zip', 'r') as zip_ref:
    zip_ref.extractall("siret")

import pandas as pd
import numpy as np

list_cols = [
  'siren', 'siret',
  'activitePrincipaleRegistreMetiersEtablissement',
  'complementAdresseEtablissement',
  'numeroVoieEtablissement',
  'typeVoieEtablissement',
  'libelleVoieEtablissement',
  'codePostalEtablissement',
  'libelleCommuneEtablissement',
  'codeCommuneEtablissement',
  'etatAdministratifEtablissement',
  'denominationUsuelleEtablissement',
  'activitePrincipaleEtablissement'
]

df = pd.read_csv(
  "siret/StockEtablissement_utf8.csv",
  usecols = list_cols)


df['numero'] = df['numeroVoieEtablissement']\
  .replace('-', np.NaN).str.split().str[0]\
  .str.extract('(\d+)', expand=False)\
  .fillna("0").astype(int)

df['numero'] = df['numero'].astype(str).replace("0","")

df['adresse'] = df['numero'] + " " + \
  df['typeVoieEtablissement'] + " " + \
  df['libelleVoieEtablissement']

df['adresse'] = df['adresse'].replace(np.nan, "")

df = df.loc[df['etatAdministratifEtablissement'] == "A"]

df.rename({"denominationUsuelleEtablissement": "denom"}, axis = "columns", inplace = True)


df_siret = df.loc[:, ['siren', 'siret','adresse', 'ape', 'denom']]

url_geoloc = "https://files.data.gouv.fr/insee-sirene-geo/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip"
r = requests.get(url_geoloc)  
with open('geoloc.zip', 'wb') as f:
    f.write(r.content)

import zipfile
import shutil

with zipfile.ZipFile('geoloc.zip', 'r') as zip_ref:
    zip_ref.extractall("geoloc")

os.remove("siret.zip")


shutil.rmtree('siret/')

df_geoloc = pd.read_csv(
  "geoloc/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv",
  usecols = ["siret", "epsg", "x", "y", "x_longitude", "y_latitude"] , sep = ";")

df_geolocalized = df_siret.merge(df_geoloc, on = "siret") 


# Importation des bases
dict_data = read_all_raw(list_bases)
dict_data.keys()


dict_data['etablissements'].iloc[0]

df['ape'] = df['activitePrincipaleEtablissement'].str.replace("\.", "", regex = True)

df.head(1)

df.columns

cols = pd.read_csv(
  "siret/StockEtablissement_utf8.csv",
  nrows=1)
cols.columns


essai = pd.read_csv(
  "siret/StockEtablissement_utf8.csv", nrows=1)
essai.columns

es.indices.delete(index='sirus_2020')

es.count(index = 'sirus_2020')

def elastic():
    """Connection avec Elastic sur le data lab"""
    es = Elasticsearch([{'host': HOST, 'port': 9200, 'scheme': 'http'}], http_compress=True, request_timeout=200)
    return es

es = elastic()

es.count(index = "sirus_2020")

dict_data = fc.read_all_raw(fc.list_bases)

es.indices.get_mapping(index = "sirus_2020")

em = dict_data["emissions"]

fullsearch = es.search(index = "sirus_2020", # l'index dans lequel on cherche
                       q = "CPCU - CENTRALE DE BERCY", # notre requête textuelle
                              size = 1) # taille de l'ensemble les échos souhaités


specificsearch = es.search(index = 'sirus_2020', body = 
'''{
  "query": {
    "bool": {
      "should":
          { "match": { "rs_denom":   "CPCU - CENTRALE DE BERCY"}},
      "filter": [
          {"geo_distance": {
                  "distance": "0.5km",
                  "location": {
                        "lat": "48.84329", 
                        "lon": "2.37396"
                              }
                            }
            }, 
            { "prefix":  { "apet": "3530" }}
                ]
            }
          }
}'''
)

# Exemple 1 (filtre dans un rayon de 10km autour d'un point, on cherche une denomination).
ex1 = es.search(index = 'sirus_2020', body = '''{
  "query": {
    "bool": {
      "must":
      { "match": { "denom":   "institut national de la statistique"}}
      ,
      "filter":
        {"geo_distance": {
          "distance": "10km",
          "location": {
            "lat": "48.8168",
            "lon": "2.3099"
          }
        }
      }
    }
  }
}
''')['hits']['hits']
ex1



es.transport.hosts