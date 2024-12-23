import json
import os
from ensurepip import bootstrap
from kafka import KafkaConsumer


def consume(mode='latest'):
    consumer = KafkaConsumer(
        os.environ['ELASTIC_TOPIC'],
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset=mode
    )

    for message in consumer:
        split_report(message.value)
