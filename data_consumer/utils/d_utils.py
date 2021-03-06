from types import NoneType
from dotenv import load_dotenv
from os import getenv, path
from loguru import logger

def check_and_load_env(func):
    """
    wrapper for checking if env exists and loading it to current work process if it exists.
    If it doesn't exists -> FileNotFoundError raised
    Tested with Psql and Kafka classes conjuration
    """
    def wrapper(self=None, *, path_to_env: str = '.env', **kwargs):
        if path.exists(path_to_env) is False:
            logger.error(f'There is no .env on your path ({path_to_env})')
            raise FileNotFoundError(f'There is no .env on your path ({path_to_env})')
        load_dotenv(path_to_env) 
        if self:
            val = func(self, **kwargs)
        else:
            val = func(**kwargs)
        return val
    return wrapper

def check_dotenv_line(data_name:str, data:str) -> bool:
    """
    checking one line from env. Criteria: it have data (not empty), it doesnt have any mistaken 
    symbols that will turn data to NoneType (checking by type)
    :param: value: str - this param is name of data in env
    :param: result: str - this is data from env
    Tested
    """
    if data == '':
        msg = f'fill .env with {data_name}, please'
        logger.error(msg)
        raise KeyError(msg)
    elif type(data) is NoneType:
        msg = f'something wrong with values({data_name}) in .env, one of them could not be parsed'
        logger.error(msg)
        raise KeyError(msg)
    return True

def get_vars_from_env(values: str | tuple | list) -> str | list:
    """
    Getting data for values from env. 
    :param: values: str | tuple | list - one or more values to be taken from env
    :return: string if one value, list if many values
    """
    if type(values) is tuple or type(values) is list:
        result = []
        for val in values:
            result_val = getenv(val)
            check_dotenv_line(val, result_val)
            result.append(result_val)
    else:
        result = getenv(values)
        check_dotenv_line(values, result)

    return result

class PsqlEnv:
    """
    Class that conjures environment variables for Psql from .env file
    """

    def __init__(self, psql_form: str | tuple | list | None = None, env_file_path: str = '.env') -> None:
        """
        :param: psql_form - if you want to create vars with your data you can put here data names to collect
        :param: env_file_path - path to .env file, usualy it lies near main file
        """
        self.name, self.user, self.password,\
            self.host, self.port, self.ca_path = self._get_psql_env(psql_form=psql_form, path_to_env=env_file_path)

    @check_and_load_env
    def _get_psql_env(self, psql_form: str | tuple | list | None = None) -> str | list:
        """
        Getting from .env variables for psql.
        :param: path_to_env: str - path to .env file (better to be full path to avoid errors)
        :return: str if one value, list if many
        """
        if not psql_form:
            psql_form = ("DATABASE", "USER", "PASSWORD", "HOST", "PORT", "CA_PATH")

        result = get_vars_from_env(psql_form)

        return result

class KafkaEnv:
    """
    Class that conjures environment variables for Kafka from .env file
    """
    def __init__(self, kafka_form: str | tuple | list | None = None, env_file_path: str = '.env') -> None:
        """
        :param: kafka_form - if you want to create vars with your data you can put here data names to collect
        :param: env_file_path - path to .env file, usualy it lies near main file
        """
        self.topic_name, self.server, self.ca_path, self.csr_path,\
            self.key_path = self._get_kafka_env(kafka_env_form=kafka_form, path_to_env=env_file_path)


    @check_and_load_env
    def _get_kafka_env(self, kafka_env_form: str | tuple | list | None = None) -> str | list:
        """
        Getting from .env variables for kafka. 
        :param: path_to_env: str - path to .env file (better to be full path to avoid errors)
        :return: str if one value, list if many
        """
        if not kafka_env_form:
            kafka_env_form = ("KAFKA_TOPIC_NAME", "KAFKA_SERVER", "KAFKA_CA_FILE_PATH", 
                              "KAFKA_CSR_FILE_PATH", "KAFKA_KEY_FILE_PATH")

        result = get_vars_from_env(kafka_env_form)

        return result

if __name__ == '__main__':
    psql_env = PsqlEnv(env_file_path="./data_consumer/.env")
    print(psql_env.ca_path)