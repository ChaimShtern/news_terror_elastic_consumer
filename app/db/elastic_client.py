from elastic_transport import SecurityWarning
from elasticsearch import Elasticsearch
import warnings

warnings.filterwarnings("ignore", category=SecurityWarning)


def elastic_search_client():
    client = Elasticsearch(
        ['http://localhost:9200'],
        basic_auth=("elastic", "123456"),
        verify_certs=False
    )
    return client


def elastic_search_index(es_client, index_name):
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body={
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2
            }
        })
    return index_name

