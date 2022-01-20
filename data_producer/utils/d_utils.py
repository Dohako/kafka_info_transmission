from types import NoneType
from dotenv import load_dotenv
from os import getenv, path
from loguru import logger

def check_and_load_env(func):
    def wrapper(self=None, *, path_to_env: str = '.env', **kwargs):
        if path.exists(path_to_env) is False:
            logger.error(f'There is no .env on your path ({path_to_env})')
            raise FileExistsError(f'There is no .env on your path ({path_to_env})')
        load_dotenv(path_to_env) 
        if self:
            val = func(self, **kwargs)
        else:
            val = func(**kwargs)
        return val
    return wrapper

def check_dotenv_line(value:str) -> bool:
    if value == '':
        logger.error(f'fill .env with {value}, please')
        raise KeyError(f'fill .env with {value}, please')
    elif type(value) is NoneType:
        logger.error('something wrong with values in .env, one of them could not be parsed')
        raise KeyError('something wrong with values in .env, one of them could not be parsed')
    return True

def get_vars_from_env(values: str | tuple) -> str | tuple:
    if type(values) is tuple:
        result = []
        for val in values:
            val = getenv(val)
            check_dotenv_line(val)
            result.append(val)
        values = result
    else:
        values = getenv(values)
        check_dotenv_line(values)
        
    return values

class KafkaEnv:

    def __init__(self, kafka_env_form: tuple | None = None, env_file_path: str = '.env') -> None:
        self.topic_name, self.server, self.ca_path, self.csr_path,\
            self.key_path = self._get_kafka_env(kafka_env_form=kafka_env_form, path_to_env=env_file_path)


    @check_and_load_env
    def _get_kafka_env(self, kafka_env_form: tuple | None = None) -> dict:
        """
        :return: 
                {
                    "KAFKA_TOPIC_NAME":...,
                    "KAFKA_SERVER":..., 
                    "KAFKA_CA_FILE_PATH"
                    "KAFKA_CSR_FILE_PATH"
                    "KAFKA_KEY_FILE_PATH"
                }
        """
        if not kafka_env_form:
            kafka_env_form = ("KAFKA_TOPIC_NAME", "KAFKA_SERVER", "KAFKA_CA_FILE_PATH", 
                              "KAFKA_CSR_FILE_PATH", "KAFKA_KEY_FILE_PATH")

        result = get_vars_from_env(kafka_env_form)

        return result

@check_and_load_env
def get_url_from_env():
    return get_vars_from_env("URL")

if __name__ == '__main__':
    ...