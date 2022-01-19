# kafka work 
from sys import api_version
from time import sleep
from json import dumps
from kafka import KafkaProducer
import ssl
import os


ca = './data_producer/utils/cert/ca.pem'
csr = './data_producer/utils/cert/access_cert.csr'
key = './data_producer/utils/cert/access_key.key'
print(os.path.isfile(ca))
print(os.path.isfile(csr))
print(os.path.isfile(key))

producer = KafkaProducer(bootstrap_servers=['dohako-aiven-kafka-dapankratev-a0b7.aivencloud.com:17386'],
                         ssl_cafile=ca,
                         ssl_certfile = csr,
                         ssl_keyfile = key,
                         security_protocol = "SSL",
                         api_version = (3,0,0),
                         ssl_check_hostname = False,
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                        )


def send_data(data:dict):
    producer.send('test', value=data)


if __name__ == "__main__":
    for e in range(1000):
        data = {'number' : e}
        producer.send('numtest', value=data)
        sleep(5)
    