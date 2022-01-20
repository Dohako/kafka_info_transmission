# kafka work 

from kafka import KafkaConsumer
from json import loads
from utils.d_utils import KafkaEnv
import os


def set_consumer() -> KafkaConsumer:
    env = KafkaEnv()
    
    check_ca = os.path.isfile(env.ca_path)
    check_csr = os.path.isfile(env.csr_path)
    check_key = os.path.isfile(env.key_path)
    if any((not check_ca, not check_csr, not check_key)):
        raise FileExistsError("not all certs are present in cert folder")
    else: 
        print("Certs in place")

    consumer = KafkaConsumer(env.topic_name, 
                            bootstrap_servers=[env.server],
                            ssl_cafile=env.ca_path,
                            ssl_certfile = env.csr_path,
                            ssl_keyfile = env.key_path,
                            security_protocol = "SSL",
                            api_version = (3,0,0),
                            ssl_check_hostname = False,
                            auto_offset_reset='earliest', 
                            enable_auto_commit=True, 
                            group_id='$Default', 
                            value_deserializer=lambda x: loads(x.decode('utf-8'))
                            )
    return consumer


if __name__ == "__main__":
    pass