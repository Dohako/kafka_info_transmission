import psycopg2
import psycopg2.errors
from .d_utils import PsqlEnv
from .sql_queries import CREATE_METRICS_TABLE, INSERT_ALL, INSERT
import datetime
from typing import Tuple

# noinspection SqlResolve
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

    # def _get_df_from_db(self, sql: str) -> pd.DataFrame:
    #     try:
    #         result = read_sql(sql, self.conn)
    #     except psycopg2.InterfaceError:
    #         self._connect()
    #         result = read_sql(sql, self.conn)
    #     except Exception:
    #         # FIXME:another pandas exception...
    #         self._connect()
    #         result = read_sql(sql, self.conn)
    #     return result

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
        ok, msg = self.check_dict(data)
        if not ok:
            return (False, msg)
        checker_time = datetime.datetime.strptime(data['checker_time'], "%d.%m.%Y %H:%M:%S")
        value = data['value'].replace(',','')
        sql = INSERT_ALL.format(data['status_code'], 
                                data['response_time_s'],
                                value,
                                checker_time)
        if not test:
            self._set_data(sql)

        return (True, '')

    def set_data(self, data:dict, test:bool=False) -> Tuple[bool, str]:
        ok, msg = self.check_dict(data, all_fields=False)
        if not ok:
            return (False, msg)
        sql = INSERT.format(data['status_code'],
                            data['checker_time'])
        if not test:
            self._set_data(sql)

        return (True, '')

    def check_dict(self, input_dict:dict, all_fields:bool = True) -> Tuple[bool, str]:
        if all_fields:
            waited_keys = ('status_code', 'response_time_s', 'value', 'checker_time')
        else:
            waited_keys = ('status_code', 'checker_time')
        for key in waited_keys:
            if key not in input_dict.keys():
                return (False, f'{key} not in input')
        for item in input_dict.items():
            for each in item:
                if type(each) is str and ';' in each:
                    return (False, f"symbol ';' is forbidden for input")

        return (True, '')

    def create_metrics_table(self):
        sql = CREATE_METRICS_TABLE
        try:
            self._set_data(sql)
        except psycopg2.Error:
            pass


if __name__ == '__main__':
    base = BaseHandler(path_to_env="./.env")
    # base._set_data("insert into test values('hello');")
    a = base._get_data("SELECT * FROM metrics;")
    print(a)
    # base._set_data("drop table test;")