# kafka work 

from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads
from d_utils import get_kafka_env

def run_consumer():
    kafka_env = get_kafka_env()
    consumer = KafkaConsumer('numtest', 
                            bootstrap_servers=['localhost:9092'],
                            auto_offset_reset='earliest', 
                            enable_auto_commit=True, 
                            group_id='my-group', 
                            value_deserializer=lambda x: loads(x.decode('utf-8'))
                            )

    for message in consumer:
        message = message.value
        print(f'Received.. {message}')