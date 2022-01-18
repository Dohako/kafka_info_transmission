from dotenv import load_dotenv
from os import getenv, path
from loguru import logger
from collections import namedtuple

def check_and_load_env(func):
    def wrapper(self=None, *, path_to_env: str = '.env', **kwargs):
        print(path_to_env)
        if path.exists(path_to_env) is False:
            logger.error(f'There is no .env on your path ({path_to_env})')
            raise FileExistsError(f'There is no .env on your path ({path_to_env})')
        load_dotenv(path_to_env) 
        val = func(self, **kwargs)
        return val
    return wrapper


def get_vars_from_env(values: str | tuple) -> dict | tuple:
        if type(values) is dict:
            for key in values.keys():
                values[key] = getenv(key)
                if values[key] == '':
                    logger.error(f'fill .env with {key}, please')
                    raise KeyError(f'fill .env with {key}, please')
        elif type(values) is tuple:
            result = []
            for val in values:
                val = getenv(val)
                if val == '':
                    logger.error(f'fill .env with {val}, please')
                    raise KeyError(f'fill .env with {val}, please')
                result.append(val)
            values = result
        else:
            values = getenv(values)
            if values == '':
                logger.error(f'fill .env with {values}, please')
                raise KeyError(f'fill .env with {values}, please')
        return values

class PsqlEnv:

    def __init__(self, psql_form: dict | None = None, env_file_path: str = '.env') -> None:
        self.name, self.user, self.password,\
            self.host, self.port = self.get_psql_env(psql_form=psql_form, path_to_env=env_file_path)

    @check_and_load_env
    def get_psql_env(self, psql_form: str | dict | tuple | None = None) -> dict:
        """
        Getting from .env variables for psql. There is two types: necessary and not. Those that are not
        will be replaced with default if .env miss them. Those, that are necessary will raise exception
        if .env miss them.
        :type path_to_env: str path to .env file (better to be full path to avoid errors)
        :return: {
                    "DATABASE":..., 
                    "USER":..., 
                    "PASSWORD":..., 
                    "HOST":..., 
                    "PORT":...
                    }
        """
        if not psql_form:
            # psql_dict = {"DATABASE":None, "USER":None, "PASSWORD":None, "HOST":None, "PORT":None}
            psql_form = ("DATABASE", "USER", "PASSWORD", "HOST", "PORT")

        psql_form = get_vars_from_env(psql_form)

        return psql_form

class KafkaEnv:

    @check_and_load_env
    def get_kafka_env(self, kafka_dict: dict = None) -> dict:
        """
        :return: 
                {
                    "KAFKA_GROUP_ID":..., 
                    "KAFKA_SERVER":..., 
                    "KAFKA_TOPIC_NAME":...
                }
        """
        if not kafka_dict:
            kafka_dict = {"KAFKA_TOPIC_NAME":None, "KAFKA_SERVER":None, "KAFKA_GROUP_ID":None}

        kafka_dict = self.get_vars_from_env(kafka_dict)

        return kafka_dict

    @check_and_load_env
    def get_url_from_env(self):
        return self.get_vars_from_env("URL")

if __name__ == '__main__':
    psql_env = PsqlEnv()
    print(psql_env.host)