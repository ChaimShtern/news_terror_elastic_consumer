from elasticsearch.helpers import bulk, BulkIndexError
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
    try:
        prepared_data = [
            {
                "_index": elastic_search_index(es_client, index_name),
                "_source": doc
            }
            for i, doc in enumerate(data)
        ]
        bulk(es_client, prepared_data)
    except BulkIndexError as e:
        print(f"{len(e.errors)} documents failed to index.")
        for error in e.errors:
            print("Error details:", error)


def delete_index(es_client, index_name):
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
        print(f"Index {index_name} has been deleted.")
    else:
        print(f"Index {index_name} does not exist.")
