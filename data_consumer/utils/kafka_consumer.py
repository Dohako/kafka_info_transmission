# kafka work 

from sys import api_version
from kafka import KafkaConsumer
from json import loads
import os

ca = './data_consumer/utils/cert/ca.pem'
csr = './data_consumer/utils/cert/access_cert.csr'
key = './data_consumer/utils/cert/access_key.key'
check_ca = os.path.isfile(ca)
check_csr = os.path.isfile(csr)
check_key = os.path.isfile(key)
if any((not check_ca, not check_csr, not check_key)):
    raise FileExistsError("not all certs are present in cert folder")
else: 
    print("Certs in place")

consumer = KafkaConsumer('test', 
                        bootstrap_servers=['dohako-aiven-kafka-dapankratev-a0b7.aivencloud.com:17386'],
                        ssl_cafile=ca,
                        ssl_certfile = csr,
                        ssl_keyfile = key,
                        security_protocol = "SSL",
                        api_version = (3,0,0),
                        ssl_check_hostname = False,
                        auto_offset_reset='earliest', 
                        enable_auto_commit=True, 
                        group_id='$Default', 
                        value_deserializer=lambda x: loads(x.decode('utf-8'))
                        )


def consume_messages() -> list:
    last_messages = []
    for message in consumer:
        message = message.value
        # last_messages.append(message)
        print(message)
    return last_messages

if __name__ == "__main__":
    consume_messages()