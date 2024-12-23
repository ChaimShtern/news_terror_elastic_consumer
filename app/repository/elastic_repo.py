from elasticsearch.helpers import bulk
from app.db.elastic_client import elastic_search_index


def create_index(es_client, index_name):
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body={
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2
            }
        })


def insert_bulk_data(es_client, index_name, data):
    prepared_data = [
        {
            "_id": f"doc_{i}",
            "_index": elastic_search_index(es_client, index_name),
            "_source": doc
        }
        for i, doc in enumerate(data)
    ]
    bulk(es_client, prepared_data)

