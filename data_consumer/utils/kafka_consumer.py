# kafka work 

from kafka import KafkaConsumer
from json import loads
from d_utils import get_kafka_env

kafka_env = get_kafka_env()
consumer = KafkaConsumer('numtest', 
                        bootstrap_servers=['localhost:9092'],
                        auto_offset_reset='earliest', 
                        enable_auto_commit=True, 
                        group_id='my-group', 
                        value_deserializer=lambda x: loads(x.decode('utf-8'))
                        )


def consume_messages() -> list:
    last_messages = []
    for message in consumer:
        message = message.value
        last_messages.append(message)
    return last_messages