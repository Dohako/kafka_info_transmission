# kafka work 
from json import dumps
from kafka import KafkaProducer, errors as KafkaError
import os
from typing import Tuple

from utils.d_utils import KafkaEnv


class Producer:
    def __init__(self) -> None:
        
        self.env = KafkaEnv()
        assert os.path.isfile(self.env.ca_path) is True, f"файл CA отсутствует по указанному пути {self.env.ca_path}"
        assert os.path.isfile(self.env.csr_path) is True, f"файл CSR отсутствует по указанному пути {self.env.csr_path}"
        assert os.path.isfile(self.env.key_path) is True, f"файл KEY отсутствует по указанному пути {self.env.key_path}"

        self.producer = KafkaProducer(bootstrap_servers=[self.env.server],
                                        ssl_cafile=self.env.ca_path,
                                        ssl_certfile = self.env.csr_path,
                                        ssl_keyfile = self.env.key_path,
                                        security_protocol = "SSL",
                                        api_version = (3,0,0),
                                        ssl_check_hostname = False,
                                        value_serializer=lambda x: dumps(x).encode('utf-8')
                                        )


    def send_data(self, data:dict) -> Tuple[bool, str]:
        try:
            self.producer.send(self.env.topic_name, value=data)
        except KafkaError.KafkaTimeoutError as error:
            return (False, error)
        return (True, '')


if __name__ == "__main__":
    ...
    