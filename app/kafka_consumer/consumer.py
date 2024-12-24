import json
import os
from ensurepip import bootstrap
from kafka import KafkaConsumer
from dotenv import load_dotenv

from app.services.map_data import map_to_elastic_format

load_dotenv(verbose=True)



def consume(mode='latest'):
    consumer = KafkaConsumer(
        os.environ['ELASTIC_TOPIC'],
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset=mode
    )

    for message in consumer:
        map_to_elastic_format(message.value)
