from dotenv import load_dotenv
from os import getenv, path
from loguru import logger


def check_env_file(path_to_env: str = '.env') -> None:
    if path.exists(path_to_env) is False:
        logger.error('There is no .env on your path')
        raise FileExistsError('There is no .env on your path')

def get_vars_from_env(val_dict:dict) -> dict:
    for key in val_dict.keys():
        val_dict[key] = getenv(key)
        if val_dict[key] == '':
            logger.error(f'fill .env with {key}, please')
            raise KeyError(f'fill .env with {key}, please')
    return val_dict

def get_psql_env(path_to_env: str = '.env', psql_dict:dict=None) -> dict:
    """
    Getting from .env variables for psql. There is two types: necessary and not. Those that are not
    will be replaced with default if .env miss them. Those, that are necessary will raise exception
    if .env miss them.
    :type path_to_env: str path to .env file (better to be full path to avoid errors)
    :return: {
                "database_name":..., 
                "psql_user":..., 
                "psql_password":..., 
                "psql_host":..., 
                "psql_port":...
                }
    """
    check_env_file(path_to_env)

    load_dotenv(path_to_env)

    if not psql_dict:
        psql_dict = {"DATABASE":None, "USER":None, "PASSWORD":None, "HOST":None, "PORT":None}

    psql_dict = get_vars_from_env(psql_dict)

    return psql_dict

def get_kafka_env(path_to_env: str = '.env', kafka_dict: dict = None) -> dict:
    check_env_file(path_to_env)
    load_dotenv(path_to_env)

    if not kafka_dict:
        kafka_dict = {"KAFKA_TOPIC_NAME":None, "KAFKA_SERVER":None, "KAFKA_GROUP_ID":None}

    kafka_dict = get_vars_from_env(kafka_dict)

    return kafka_dict