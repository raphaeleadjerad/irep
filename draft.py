import functions as fc
from elasticsearch import Elasticsearch
HOST = 'elasticsearch-master.projet-ssplab'


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