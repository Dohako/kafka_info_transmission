import psycopg2
import psycopg2.errors
import datetime
from typing import Tuple

from .d_utils import PsqlEnv
from .sql_queries import CREATE_METRICS_TABLE, INSERT_ALL, INSERT


class BaseHandler:
    def __init__(self, path_to_env='.env'):
        self.env = PsqlEnv(env_file_path=path_to_env)

        self.conn = None
        try:
            self._connect()
        except psycopg2.Error as ex:
            print(ex)
            raise ex

    def _get_data(self, sql: str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except psycopg2.Error:
            self._connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def _connect(self):
        self.conn = psycopg2.connect(database=self.env.name, user=self.env.user,
                           password=self.env.password, host=self.env.host, port=self.env.port)
        self.conn.autocommit = True

    def _set_data(self, sql: str) -> None:
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except psycopg2.Error:
            self._connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
        self.conn.commit()

    def set_all_data(self, data:dict, test:bool=False) -> Tuple[bool, str]:
        ok, msg, data = self.check_dict(data)
        if not ok:
            return (False, msg)
        
        local_time = datetime.datetime.now()

        sql = INSERT_ALL.format(data['status_code'], 
                                data['response_time_s'],
                                data['value'],
                                data['checker_time'],
                                local_time)
        if not test:
            self._set_data(sql)

        return (True, msg)

    def set_data(self, data:dict, test:bool=False) -> Tuple[bool, str]:
        ok, msg, data = self.check_dict(data, all_fields=False)
        if not ok:
            return (False, msg)
        sql = INSERT.format(data['status_code'],
                            data['checker_time'])
        if not test:
            self._set_data(sql)

        return (True, '')

    def check_dict(self, input_dict:dict, all_fields:bool = True) -> Tuple[bool, str, dict | None]:
        if all_fields:
            waited_keys = ('status_code', 'response_time_s', 'value', 'checker_time')
        else:
            waited_keys = ('status_code', 'checker_time')

        for key in waited_keys:
            if key not in input_dict.keys():
                return (False, f'{key} not in input', None)

        for item in input_dict.items():
            for each in item:
                if type(each) is str and ';' in each:
                    return (False, f"symbol ';' is forbidden for input", None)

        msg = ''
        # optional fields
        if all_fields:
            if type(input_dict['value']) is not float:
                msg += f'value was given with wrong format (Expected {type(1.0)}, got {type(input_dict["value"])}) \n'
                input_dict['value'] = -1.0

            if type(input_dict['response_time_s']) is not float:
                msg += f'response_time_s was given with wrong format (Expected {type(1.0)}, got {type(input_dict["response_time_s"])}) \n'
                input_dict['response_time_s'] = -1.0

        # mandatory fields
        if type(input_dict['status_code']) is not int:
            msg += f'status_code was given with wrong format (Expected {type(1)}, got {type(input_dict["status_code"])}) \n'
            input_dict['status_code'] = -1

        if type(input_dict['checker_time']) is str:
            #TODO checking inside string
            input_dict['checker_time'] = datetime.datetime.strptime(input_dict['checker_time'], "%d.%m.%Y %H:%M:%S")
        else:
            msg += f'checker_time was given with wrong format (Expected {type("1")}, got {type(input_dict["checker_time"])}) \n'
            input_dict['checker_time'] = datetime.datetime.now()

        return (True, msg, input_dict)

    def create_metrics_table(self):
        sql = CREATE_METRICS_TABLE
        try:
            self._set_data(sql)
        except psycopg2.Error:
            pass


if __name__ == '__main__':

    base = BaseHandler(path_to_env="./.env")
    # base._set_data("insert into test values('hello');")
    # a = base._get_data("SELECT * FROM metrics;")
    # print(a)
    base._set_data("drop table test;")