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


df.iloc[0]
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

df.rename(
  {"denominationUsuelleEtablissement": "denom",
  "libelleCommuneEtablissement": "commune",
  "codeCommuneEtablissement" : "code_commune",
  "codePostalEtablissement": "code_postal"},
  axis = "columns", inplace = True)

df['ape'] = df['activitePrincipaleEtablissement'].str.replace("\.", "", regex = True)
df['denom'] = df["denom"].replace(np.nan, "")

df_siret = df.loc[:, ['siren', 'siret','adresse', 'ape', 'denom', 'commune', 'code_commune','code_postal']]
df_siret['code_postal'] = df_siret['code_postal'].replace(np.nan, "0").astype(int).astype(str).replace("0","")


import requests
url_geoloc = "https://files.data.gouv.fr/insee-sirene-geo/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip"
r = requests.get(url_geoloc)  
with open('geoloc.zip', 'wb') as f:
    f.write(r.content)

import zipfile
import shutil
import os 


os.remove("siret.zip")
shutil.rmtree('siret/')

with zipfile.ZipFile('geoloc.zip', 'r') as zip_ref:
    zip_ref.extractall("geoloc")

df_geoloc = pd.read_csv(
  "geoloc/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv",
  usecols = ["siret", "epsg", "x_longitude", "y_latitude"] , sep = ";")

df_geolocalized = df_siret.merge(df_geoloc, on = "siret") 


# Importation des bases
dict_data = read_all_raw(list_bases)
dict_data.keys()


mapping = es_ssplab.indices.get_mapping(index='sirus_2020')
mapping["sirus_2020_e_3_ngr_bool"]["mappings"]["properties"].keys()
result = es_ssplab.search(index="sirus_2020", body={"query": {"match_all": {}}})

mapping["sirus_2020_e_3_ngr_bool"]["mappings"]["properties"]["adr_et_loc_geo"]

df_geolocalized.columns

# indexation simple

string_var = ["adresse", "denom", "ape", "commune"]
#map_string = {'type': 'text', 'fields': {'ngr': {'type': 'text', 'analyzer': 'ngram_analyzer'}, 'stem': {'type': 'text', 'norms': False, 'analyzer': 'stemming', 'search_analyzer': 'search_syn_rs'}}, 'norms': False}
map_string = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
mapping_string = {l: map_string for l in string_var}

# geoloc
mapping_geoloc = {
  "location": {
    "type": "geo_point"
    }
}    

# keywords
keyword_var = ["siren","siret","code_commune","code_postal"]
map_keywords = {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
mapping_keywords = {l: map_keywords for l in keyword_var}

# mapping
mapping_elastic = {"mappings":
  {"properties":
    {**mapping_string, **mapping_geoloc, **mapping_keywords}
  }
}

# mapping
if es.indices.exists('sirene'):
    es.indices.delete('sirene')

es.indices.create(index = "sirene", body = mapping_elastic)   


def gen_dict_from_pandas(index_name, df):
    '''
    Lit un dataframe pandas Open Food Facts, renvoi un itérable = dictionnaire des données à indexer, sous l'index fourni
    '''
    for i, row in df.iterrows():
        header= {"_op_type": "index","_index": index_name,"_id": i}
        yield {**header,**row}

df_geolocalized['location'] = df_geolocalized['y_latitude'].astype(str) + ", " + df_geolocalized['x_longitude'].astype(str)


from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque
deque(parallel_bulk(client=es, actions=gen_dict_from_pandas("sirene", df_geolocalized.head(5000)), chunk_size = 1000, thread_count = 4))

es_dict = gen_dict_from_pandas("sirene", df_geolocalized.head(10))

list(es_dict)

result = es_ssplab.search(index="sirus_2020", body={"query": {"match_all": {}}})
result["hits"]['hits'][0]['_source']

# indexation --version non fonctionnelle

# mapping
if es.indices.exists('sirene'):
    es.indices.delete('sirene')


es_ssplab = Elasticsearch([{'host': 'elasticsearch-master.projet-ssplab', 'port': 9200, 'scheme': 'http'}], http_compress=True, request_timeout=200)
es = Elasticsearch([{'host': 'elasticsearch-master', 'port': 9200, 'scheme': 'http'}], http_compress=True, request_timeout=200)


# string_var
string_var = ["adresse", "denom"]
map_string = {'type': 'text', 'fields': {'ngr': {'type': 'text', 'analyzer': 'ngram_analyzer'}, 'stem': {'type': 'text', 'norms': False, 'analyzer': 'stemming', 'search_analyzer': 'search_syn_rs'}}, 'norms': False}
mapping_string = {l: map_string for l in string_var}


map_es['sirus_2020_e_3_ngr_bool']['mappings']['properties'].keys()

map_es['sirus_2020_e_3_ngr_bool']['mappings']['properties']['location']
truc['sirus_2020_e_3_ngr_bool']['settings']

# geoloc
mapping_geoloc = {"geo": {
             "properties": {
                 "location": {
                     "type": "geo_point"
                 }
             }
         }
}    

# keywords
keyword_var = ["siren","siret"]
map_keywords = {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
mapping_keywords = {l: map_keywords for l in keyword_var}

# ape
map_ape = {'type': 'text', 'fields': {'2car': {'type': 'text', 'analyzer': 'trunc2'}, '3car': {'type': 'text', 'analyzer': 'trunc3'}, '4car': {'type': 'text', 'analyzer': 'trunc4'}, '5car': {'type': 'text', 'analyzer': 'trunc5'}, 'div_naf': {'type': 'text', 'analyzer': 'trunc2'}, 'keyword': {'type': 'keyword', 'ignore_above': 256}, 'ngr': {'type': 'text', 'analyzer': 'ngram_analyzer'}}}
mapping_ape = {"ape": map_ape}

truc = es_ssplab.indices.get_settings(index = "sirus_2020")
map_es = es_ssplab.indices.get_mapping(index = "sirus_2020")

elastic_filters = {
  'filter': {
    'trunc5': {'length': '5', 'type': 'truncate'}, 'trunc4': {'length': '4', 'type': 'truncate'}, 'trunc3': {'length': '3', 'type': 'truncate'}, 'trunc2': {'length': '2', 'type': 'truncate'}, 'french_stemmer': {'type': 'stemmer', 'language': 'light_french'}
  }
}
elastic_analyzers = {
  {
    'ngram_analyzer': {'filter': 'lowercase', 'tokenizer': 'ngram_tokenizer'}
  }
}
elastic_tokenizers = {
  {'ngram_tokenizer': {'token_chars': ['letter', 'digit'], 'min_gram': '3', 'type': 'ngram', 'max_gram': '3'}}
}


mapping_template = {
  "settings":
    {
      "index":
      {
        "number_of_replicas": 2,
        "analysis": {
          "filter": {
            "french_stemmer": {"type": "stemmer", "language": "light_french"},
            "french_stop": {"type": "stop", "language": "_french_"},
            "trunc2": {"length": "2", "type": "truncate"},
            "trunc3": {"length": "3", "type": "truncate"},
            "trunc4": {"length": "4", "type": "truncate"},
            "trunc5": {"length": "5", "type": "truncate"},
            "trunc7": {"length": "7", "type": "truncate"},
            "trunc9": {"length": "9", "type": "truncate"}},
            "analyzer": {
              "ngram_analyzer": {"filter": "lowercase", "tokenizer": "ngram_tokenizer"},
              "stem_analyzer": {"filter": ["french_stop", "french_stemmer"], "tokenizer": "standard"},
              "trunc2_analyzer": {"filter": "trunc2", "tokenizer": "keyword"},
              "trunc3_analyzer": {"filter": "trunc3", "tokenizer": "keyword"},
              "trunc4_analyzer": {"filter": "trunc4", "tokenizer": "keyword"},
              "trunc5_analyzer": {"filter": "trunc5", "tokenizer": "keyword"},
              "trunc7_analyzer": {"filter": "trunc7", "tokenizer": "keyword"},
              "trunc9_analyzer": {"filter": "trunc9", "tokenizer": "keyword"}},
              "tokenizer": {"ngram_tokenizer": {"type": "ngram", "min_gram": 3, "max_gram": 4, "token_chars": "letter"}}
          }
        }
      }
    }
mapping_template["settings"]["index"]["mappings"] = {
  "properties":
    {**mapping_string, **mapping_geoloc, **mapping_keywords, **mapping_ape}
}

"mappings": {"properties": {"index": {"type": "keyword"}, "code": {"type": "keyword"}, "product_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}, "ngr": {"type": "text", "analyzer": "ngram_analyzer"}, "stem": {"type": "text", "analyzer": "stem_analyzer"}}}, "libel_clean_OFF": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}, "ngr": {"type": "text", "analyzer": "ngram_analyzer"}, "stem": {"type": "text", "analyzer": "stem_analyzer"}}}, "tokenized_off": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}, "ngr": {"type": "text", "analyzer": "ngram_analyzer"}, "stem": {"type": "text", "analyzer": "stem_analyzer"}}}, "nutriscore_grade": {"type": "keyword"}, "nova_group": {"type": "keyword"}, "divisionMatch": {"type": "text", "fields": {"5car": {"type": "text", "analyzer": "trunc5_analyzer"}, "7car": {"type": "text", "analyzer": "trunc7_analyzer"}, "9car": {"type": "text", "analyzer": "trunc9_analyzer"}, "keyword": {"type": "keyword", "ignore_above": 256}}}, "prediction": {"type": "text", "fields": {"5car": {"type": "text", "analyzer": "trunc5_analyzer"}, "7car": {"type": "text", "analyzer": "trunc7_analyzer"}, "9car": {"type": "text", "analyzer": "trunc9_analyzer"}, "keyword": {"type": "keyword", "ignore_above": 256}}}, "nutriscore_score": {"type": "float", "ignore_malformed": true}, "energy_100g": {"type": "float", "ignore_malformed": true}, "fat_100g": {"type": "float", "ignore_malformed": true}, "saturated-fat_100g": {"type": "float", "ignore_malformed": true}, "cholesterol_100g": {"type": "float", "ignore_malformed": true}, "sugars_100g": {"type": "float", "ignore_malformed": true}, "fiber_100g": {"type": "float", "ignore_malformed": true}, "proteins_100g": {"type": "float", "ignore_malformed": true}, "salt_100g": {"type": "float", "ignore_malformed": true}, "sodium_100g": {"type": "float", "ignore_malformed": true}, "calcium_100g": {"type": "float", "ignore_malformed": true}, "iron_100g": {"type": "float", "ignore_malformed": true}, "glycemic-index_100g": {"type": "float", "ignore_malformed": true}}}}
{'filter': {'french_stop': {'type': 'stop', 'stopwords': '_french_'}, 'acronymizer': {'pattern': '(?<=\\w)[A-z]*(\\s|\x08|\\-)', 'replace': '', 'type': 'pattern_replace'}, 'cleansigle': {'pattern': '\\.|\\-|\\s*', 'replace': '', 'type': 'pattern_replace'}, 'french_elision': {'type': 'elision', 'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c'], 'articles_case': 'true'}, 'trunc5': {'length': '5', 'type': 'truncate'}, 'trunc4': {'length': '4', 'type': 'truncate'}, 'trunc3': {'length': '3', 'type': 'truncate'}, 'trunc2': {'length': '2', 'type': 'truncate'}, 'french_stemmer': {'type': 'stemmer', 'language': 'light_french'}, 'pattern_stop': {'pattern': "\x08le\x08|\x08la\x08|\x08les\x08|\x08de\x08|\x08des\x08|\x08du\x08|\x08en\x08|l'|d'", 'replace': '', 'type': 'pattern_replace'}, 'toklen3': {'type': 'length', 'min': '3'}, 'french_keywords': {'keywords': ['Exemple'], 'type': 'keyword_marker'}}, 'analyzer': {'ngram_analyzer': {'filter': 'lowercase', 'tokenizer': 'ngram_tokenizer'}, 'acronym': {'filter': ['asciifolding', 'lowercase', 'pattern_stop', 'acronymizer', 'cleansigle', 'toklen3'], 'type': 'custom', 'tokenizer': 'keyword'}, 'search_syn_rs': {'filter': ['asciifolding', 'french_elision', 'lowercase', 'french_stop', 'french_stemmer'], 'tokenizer': 'letter'}, 'trunc5': {'filter': 'trunc5', 'tokenizer': 'keyword'}, 'trunc4': {'filter': 'trunc4', 'tokenizer': 'keyword'}, 'stemming': {'filter': ['asciifolding', 'french_elision', 'lowercase', 'french_stop', 'french_stemmer', 'unique'], 'tokenizer': 'letter'}, 'trunc3': {'filter': 'trunc3', 'tokenizer': 'keyword'}, 'trunc2': {'filter': 'trunc2', 'tokenizer': 'keyword'}, 'sigle': {'filter': ['classic'], 'type': 'custom', 'tokenizer': 'keyword'}}, 'tokenizer': {'ngram_tokenizer': {'token_chars': ['letter', 'digit'], 'min_gram': '3', 'type': 'ngram', 'max_gram': '3'}}}

#mapping
mapping_elastic = {"mappings":
  {"properties":
    {**mapping_string, **mapping_geoloc, **mapping_keywords, **mapping_ape}
  }
}

es_ssplab.indices.get_mapping(index = "sirus_2020")
es_ssplab.indices.get_settings(index = "sirus_2020")

truc['sirus_2020_e_3_ngr_bool']['settings']['index']['mappings']['properties']
mapping_template['settings']['index']['mappings']['properties'].keys()

es.indices.create(index = "sirene", body = mapping_template)   


keyword_var = ["siren","siret", "ape", "epsg"]
locs_var = ['y_latitude', 'x_longitude']


es_entries['geo'] = { 'location': str(df_geolocalized['x_longitude'])+","+str(df_geolocalized['y_latitude'])}

mappings = {
    "properties": {
        "geo": {
             "properties": {
                 "location": {
                     "type": "geo_point"
                 }
             }
         }
    }
}

# keyword_var
{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}

# string_var
map_string = {'type': 'text', 'fields': {'ngr': {'type': 'text', 'analyzer': 'ngram_analyzer'}, 'stem': {'type': 'text', 'norms': False, 'analyzer': 'stemming', 'search_analyzer': 'search_syn_rs'}}, 'norms': False}

# ape
{'type': 'text', 'fields': {'2car': {'type': 'text', 'analyzer': 'trunc2'}, '3car': {'type': 'text', 'analyzer': 'trunc3'}, '4car': {'type': 'text', 'analyzer': 'trunc4'}, '5car': {'type': 'text', 'analyzer': 'trunc5'}, 'div_naf': {'type': 'text', 'analyzer': 'trunc2'}, 'keyword': {'type': 'keyword', 'ignore_above': 256}, 'ngr': {'type': 'text', 'analyzer': 'ngram_analyzer'}}}

# location
{'type': 'geo_point', 'ignore_malformed': True}


{ "settings": {
  "index" : {
    "number_of_replicas" : 2
  }
},  "mappings": {"properties": {"code": {"type": "text"}, "product_name": {"type": "text"}, "abbreviated_product_name": {"type": "text"}, "generic_name": {"type": "text"}, "quantity": {"type": "text"}, "brands_tags": {"type": "text"}, "categories_tags": {"type": "text"}, "origins_tags": {"type": "text"}, "origins_en": {"type": "text"}, "stores": {"type": "text"}, "countries_tags": {"type": "text"}, "allergens_en": {"type": "float"}, "traces_en": {"type": "text"}, "serving_size": {"type": "text"}, "serving_quantity": {"type": "float"}, "no_nutriments": {"type": "float"}, "additives_n": {"type": "float"}, "additives": {"type": "float"}, "additives_tags": {"type": "text"}, "additives_en": {"type": "text"}, "ingredients_from_palm_oil_n": {"type": "float"}, "ingredients_from_palm_oil": {"type": "float"}, "ingredients_from_palm_oil_tags": {"type": "text"}, "ingredients_that_may_be_from_palm_oil_n": {"type": "float"}, "ingredients_that_may_be_from_palm_oil": {"type": "float"}, "ingredients_that_may_be_from_palm_oil_tags": {"type": "text"}, "nutriscore_score": {"type": "float"}, "nutriscore_grade": {"type": "text"}, "nova_group": {"type": "float"}, "pnns_groups_1": {"type": "text"}, "pnns_groups_2": {"type": "text"}, "states_en": {"type": "text"}, "brand_owner": {"type": "text"}, "main_category": {"type": "text"}, "main_category_en": {"type": "text"}, "energy-kj_100g": {"type": "float"}, "energy-kcal_100g": {"type": "float"}, "energy_100g": {"type": "float"}, "energy-from-fat_100g": {"type": "float"}, "fat_100g": {"type": "float"}, "saturated-fat_100g": {"type": "float"}, "-butyric-acid_100g": {"type": "float"}, "-caproic-acid_100g": {"type": "float"}, "-caprylic-acid_100g": {"type": "float"}, "-capric-acid_100g": {"type": "float"}, "-lauric-acid_100g": {"type": "float"}, "-myristic-acid_100g": {"type": "float"}, "-palmitic-acid_100g": {"type": "float"}, "-stearic-acid_100g": {"type": "float"}, "-arachidic-acid_100g": {"type": "float"}, "-behenic-acid_100g": {"type": "float"}, "-lignoceric-acid_100g": {"type": "float"}, "-cerotic-acid_100g": {"type": "float"}, "-montanic-acid_100g": {"type": "float"}, "-melissic-acid_100g": {"type": "float"}, "monounsaturated-fat_100g": {"type": "float"}, "polyunsaturated-fat_100g": {"type": "float"}, "omega-3-fat_100g": {"type": "float"}, "-alpha-linolenic-acid_100g": {"type": "float"}, "-eicosapentaenoic-acid_100g": {"type": "float"}, "-docosahexaenoic-acid_100g": {"type": "float"}, "omega-6-fat_100g": {"type": "float"}, "-linoleic-acid_100g": {"type": "float"}, "-arachidonic-acid_100g": {"type": "float"}, "-gamma-linolenic-acid_100g": {"type": "float"}, "-dihomo-gamma-linolenic-acid_100g": {"type": "float"}, "omega-9-fat_100g": {"type": "float"}, "-oleic-acid_100g": {"type": "float"}, "-elaidic-acid_100g": {"type": "float"}, "-gondoic-acid_100g": {"type": "float"}, "-mead-acid_100g": {"type": "float"}, "-erucic-acid_100g": {"type": "float"}, "-nervonic-acid_100g": {"type": "float"}, "trans-fat_100g": {"type": "float"}, "cholesterol_100g": {"type": "float"}, "carbohydrates_100g": {"type": "float"}, "sugars_100g": {"type": "float"}, "-sucrose_100g": {"type": "float"}, "-glucose_100g": {"type": "float"}, "-fructose_100g": {"type": "float"}, "-lactose_100g": {"type": "float"}, "-maltose_100g": {"type": "float"}, "-maltodextrins_100g": {"type": "float"}, "starch_100g": {"type": "float"}, "polyols_100g": {"type": "float"}, "fiber_100g": {"type": "float"}, "-soluble-fiber_100g": {"type": "float"}, "-insoluble-fiber_100g": {"type": "float"}, "proteins_100g": {"type": "float"}, "casein_100g": {"type": "float"}, "serum-proteins_100g": {"type": "float"}, "nucleotides_100g": {"type": "float"}, "salt_100g": {"type": "float"}, "sodium_100g": {"type": "float"}, "alcohol_100g": {"type": "float"}, "vitamin-a_100g": {"type": "float"}, "beta-carotene_100g": {"type": "float"}, "vitamin-d_100g": {"type": "float"}, "vitamin-e_100g": {"type": "float"}, "vitamin-k_100g": {"type": "float"}, "vitamin-c_100g": {"type": "float"}, "vitamin-b1_100g": {"type": "float"}, "vitamin-b2_100g": {"type": "float"}, "vitamin-pp_100g": {"type": "float"}, "vitamin-b6_100g": {"type": "float"}, "vitamin-b9_100g": {"type": "float"}, "folates_100g": {"type": "float"}, "vitamin-b12_100g": {"type": "float"}, "biotin_100g": {"type": "float"}, "pantothenic-acid_100g": {"type": "float"}, "silica_100g": {"type": "float"}, "bicarbonate_100g": {"type": "float"}, "potassium_100g": {"type": "float"}, "chloride_100g": {"type": "float"}, "calcium_100g": {"type": "float"}, "phosphorus_100g": {"type": "float"}, "iron_100g": {"type": "float"}, "magnesium_100g": {"type": "float"}, "zinc_100g": {"type": "float"}, "copper_100g": {"type": "float"}, "manganese_100g": {"type": "float"}, "fluoride_100g": {"type": "float"}, "selenium_100g": {"type": "float"}, "chromium_100g": {"type": "float"}, "molybdenum_100g": {"type": "float"}, "iodine_100g": {"type": "float"}, "caffeine_100g": {"type": "float"}, "taurine_100g": {"type": "float"}, "ph_100g": {"type": "float"}, "fruits-vegetables-nuts_100g": {"type": "float"}, "fruits-vegetables-nuts-dried_100g": {"type": "float"}, "fruits-vegetables-nuts-estimate_100g": {"type": "float"}, "collagen-meat-protein-ratio_100g": {"type": "float"}, "cocoa_100g": {"type": "float"}, "chlorophyl_100g": {"type": "float"}, "carbon-footprint_100g": {"type": "float"}, "carbon-footprint-from-meat-or-fish_100g": {"type": "float"}, "nutrition-score-fr_100g": {"type": "float"}, "nutrition-score-uk_100g": {"type": "float"}, "glycemic-index_100g": {"type": "float"}, "water-hardness_100g": {"type": "float"}, "choline_100g": {"type": "float"}, "phylloquinone_100g": {"type": "float"}, "beta-glucan_100g": {"type": "float"}, "inositol_100g": {"type": "float"}, "carnitine_100g": {"type": "float"}, "label": {"type": "text"}}}}



index_elastic(es =es, index_name = "openfood",
        setting_file = 'schema.json',
        df = openfood[['product_name',"libel_clean","energy_100g","nutriscore_score"]].drop_duplicates())


dict_data['etablissements'].iloc[0]

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