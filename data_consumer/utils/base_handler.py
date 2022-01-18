import psycopg2
from psycopg2 import connect
import psycopg2.errors

import redis
from json import loads, dumps
from dotenv import load_dotenv
from os import getenv

if not load_dotenv():
    raise FileNotFoundError("There is no .env file")

redis_host = getenv("REDIS_HOST")
redis_port = getenv("REDIS_PORT")
redis_db = getenv("REDIS_DB")

database_name = getenv("DATABASE")

# TODO check chapters folders in bot_structure and create tables based on it's counts



# noinspection SqlResolve
class BaseHandler:
    def __init__(self, db, path_to_env='.env'):
        self.db, self.user, self.password, self.host, self.port = get_psql_env(path_to_env=path_to_env)
        self.conn = None
        try:
            self._connect()
        except psycopg2.Error:

            print(self.db, self.user, self.password, self.host, self.port)
            # conn = connect(database="postgres", user=self.user,
            #                password=self.password, host=self.host, port=self.port)
            # conn.autocommit = True
            # cursor = conn.cursor()
            # sql = f"drop database {self.db};"
            # cursor.execute(sql)
            # conn.close()

            conn = connect(database="postgres", user=self.user,
                           password=self.password, host=self.host, port=self.port)
            conn.autocommit = True

            cursor = conn.cursor()
            sql = CREATE_DB
            cursor.execute(sql)
            conn.close()

            self._connect()
            self._set_data(CREATE_TABLE_PERSON)
            self._set_data(CREATE_TIME_ANALYTICS)

    def __del__(self):
        # self.conn.close()
        print("closed")

    def test_connect(self):
        conn = connect(database="postgres", user=self.user,
                           password=self.password, host=self.host, port=self.port)
        conn.autocommit = True

        cursor = conn.cursor()
        sql = "create database predlog_test;"
        cursor.execute(sql)
        # result = cursor.fetchall()
        # return result

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
        self.conn = connect(database=self.db, user=self.user,
                           password=self.password, host=self.host, port=self.port)
        self.conn.autocommit = True

    def _get_df_from_db(self, sql: str) -> pd.DataFrame:
        try:
            result = read_sql(sql, self.conn)
        except psycopg2.InterfaceError:
            self._connect()
            result = read_sql(sql, self.conn)
        except Exception:
            # FIXME:another pandas exception...
            self._connect()
            result = read_sql(sql, self.conn)
        return result

    def _set_data(self, sql: str) -> None:
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except psycopg2.Error:
            self._connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
        self.conn.commit()

    def insert_person(self, telegram_id, name='null', stage='null', last_seen=f"'{datetime.datetime.now()}'"):
        """
        Inserting a person in person table inside predlog DB.\n
        If a person table doesn't exists: create one and others too.\n

        :param telegram_id:
        :param name:
        :param stage:
        :param last_seen:
        :return:
        """
        if "'" not in name and name != 'null':
            name = f"'{name}'"
        if "'" not in stage and stage != 'null':
            stage = f"'{stage}'"
        sql_command = f"insert into person(telegram_id, name, stage, last_seen) " \
                      f"values({telegram_id}, {name}, {stage}, {last_seen});"

        try:
            self._set_data(sql_command)
        except psycopg2.Error as ex:
            if "already exists" in str(ex) or "уже существует" in str(ex):
                raise KeyError("Данный ключ (telegram_id) уже существует в базе")
            # psycopg не позволяет точно выделять ошибки, поэтому используется такой метод
            # по предположению, здесь может быть две ошибки: ошибка ключа и отсутствие базы
            self._set_data(CREATE_TABLE_PERSON)
            self._set_data(sql_command)
            # кроме того создаем таблицу для аналитики временной
            self._set_data(CREATE_TIME_ANALYTICS)
        for table in ERROR_TABLES:
            sql_command = f"insert into {table}(telegram_id) values ({telegram_id});"
            try:
                self._set_data(sql_command)
            except psycopg2.Error:
                self._set_data(CREATE_TABLE.format(table))
                self._set_data(sql_command)

    def update_error_tables(self, telegram_id, lesson_key: str, err_incr: int = None, reset_err: bool = None,
                            set_lesson_cleared: bool = None) -> None:
        """
        Updating data in error tables. If there isn't lesson_key column - it will create one.\n
        :param reset_err:
        :param set_lesson_cleared:
        :param telegram_id:
        :param lesson_key: c1_ch1_l1; c2_ch4_l12
        :param err_incr:
        :return:
        """
        chapter_number = int(lesson_key.split('_')[0].replace('c', ''))
        table = ERROR_TABLES[chapter_number - 1]

        try:
            prev_error_count = self.get_person_error_in_lesson(table=table, lesson_key=lesson_key,
                                                               telegram_id=telegram_id)
        except:
            # Exception from pandas, don't know how to handle it normally yet
            sql_add_col = f"alter table {table} add column {lesson_key}_cleared bool default false;"
            self._set_data(sql_add_col)
            sql_add_col = f"alter table {table} add column {lesson_key} integer default 0;"
            self._set_data(sql_add_col)
            prev_error_count = self.get_person_error_in_lesson(table=table, lesson_key=lesson_key,
                                                               telegram_id=telegram_id)
        # if error_counts:
        #     sql_command = f"update {table} set {lesson_key}={error_counts+previously_error_count} where telegram_id = {telegram_id};"
        # else:
        #     prev_err = self.get_person_error_in_lesson(table=table, lesson_key=lesson_key, telegram_id=telegram_id)
        #     sql_command = f"update {table} set {lesson_key}={error_counts+previously_error_count} where telegram_id = {telegram_id};"
        if reset_err:
            sql_command = f"update {table} set {lesson_key}={0} where telegram_id = {telegram_id};"
            self._set_data(sql_command)
        elif err_incr is not None:
            sql_command = f"update {table} set {lesson_key}={err_incr + prev_error_count} where telegram_id = {telegram_id};"
            self._set_data(sql_command)
        if set_lesson_cleared is not None:
            sql_command = f"update {table} set {lesson_key}_cleared={set_lesson_cleared} where telegram_id = {telegram_id};"
            self._set_data(sql_command)

    def get_person_error_in_lesson(self, lesson_key, telegram_id, table=None):
        if lesson_key and table is None:
            chapter_number = int(lesson_key.split('_')[0].replace('c', ''))
            table = ERROR_TABLES[chapter_number - 1]
        error_count = int(
            self._get_df_from_db(SQL_GET_ERRORS_FROM_LESSON.format(lesson_key, table, telegram_id)).iloc[0][0])
        return error_count

    def is_person_lesson_cleared(self, lesson_key, telegram_id, table=None) -> bool:
        if lesson_key and table is None:
            chapter_number = int(lesson_key.split('_')[0].replace('c', ''))
            table = ERROR_TABLES[chapter_number - 1]
        sql = f"select {lesson_key}_cleared from {table} where telegram_id = {telegram_id};"
        try:
            res = bool(self._get_df_from_db(sql).iloc[0][0])
        except Exception as ex:
            Warning(ex)
            self.update_error_tables(telegram_id=telegram_id, lesson_key=lesson_key)
            res = False
        return res

    def get_person_yo_count(self, telegram_id) -> int:
        how_many_yo_have_person = int(self._get_df_from_db(SQL_GET_COUNT_YO_FROM_PERSON.format(telegram_id)).iloc[0][0])
        return how_many_yo_have_person

    def update_person(self, telegram_id, stage: str = None, last_seen=None,
                      current_lesson_key=None, current_lesson_stage=None, current_answer_type=None, count_yo=None,
                      lesson_stage_incr: int = None) -> None:
        if current_lesson_key and "'" not in current_lesson_key:
            current_lesson_key = f"'{current_lesson_key}'"
        if stage and "'" not in stage:
            stage = f"'{stage}'"
        if current_answer_type and "'" not in current_answer_type:
            current_answer_type = f"'{current_answer_type}'"

        if not last_seen:
            last_seen=f"'{datetime.datetime.now()}'"

        sql_command = "update person set "
        if count_yo:
            how_many_yo_have_person = self.get_person_yo_count(telegram_id)
            sql_command += f"count_yo = {how_many_yo_have_person + count_yo}, "
        if lesson_stage_incr and not current_lesson_stage:
            prev_lesson_stage = self.get_person_lesson_stage(telegram_id)
            sql_command += f"current_lesson_stage = {prev_lesson_stage + lesson_stage_incr}, "
        for field, field_name in zip([stage, current_lesson_key, current_lesson_stage, current_answer_type, last_seen],
                                     ["stage", "current_lesson_key", "current_lesson_stage", "current_answer",
                                      "last_seen"]):
            if field is not None:
                sql_command += f"{field_name} = {field}"
                if field_name != "last_seen":
                    sql_command += ', '

        sql_command += f" where telegram_id = {telegram_id};"
        print(sql_command)
        try:
            self._set_data(sql_command)
        except psycopg2.Error as ex:
            if "already exists" in str(ex) or "уже существует" in str(ex):
                raise KeyError("Данный ключ (telegram_id) уже существует в базе")
            else:
                raise

    def get_person_current_answer(self, telegram_id) -> str:
        """
        Getting current answer type to choose next actions
        :param telegram_id:
        :return:
        """
        sql_command = f"select current_answer from person where telegram_id={telegram_id}"
        current_answer = str(self._get_df_from_db(sql_command).iloc[0][0])
        return current_answer

    def get_person_current_fields(self, telegram_id) -> pd.DataFrame:
        """
        get all columns from table person\n
        :param telegram_id:
        :return:
        """
        sql_command = f"select * from person where telegram_id={telegram_id}"
        return self._get_df_from_db(sql_command)

    def get_person_current_stage(self, telegram_id) -> str:
        sql_command = f"select stage from person where telegram_id={telegram_id};"
        print(sql_command)
        data = self._get_df_from_db(sql_command)
        print(data)
        current_stage = str(data.iloc[0][0])
        return current_stage

    def get_person_lesson_key(self, telegram_id) -> str:
        sql_command = f"select current_lesson_key from person where telegram_id={telegram_id}"
        current_lesson_key = str(self._get_df_from_db(sql_command).iloc[0][0])
        return current_lesson_key

    def get_person_lesson_stage(self, telegram_id) -> int:
        sql_command = f"select current_lesson_stage from person where telegram_id={telegram_id}"
        current_lesson_stage = int(self._get_df_from_db(sql_command).iloc[0][0])
        return current_lesson_stage

    def check_person_exists(self, telegram_id) -> bool:
        sql_command = f"select exists(select 1 from person where telegram_id={telegram_id});"
        result = bool(read_sql(sql_command, self.conn).iloc[0][0])
        return result

    def get_person_lessons_statistics(self, telegram_id):
        count_true = 0
        count_err = 0
        max_err_lesson = ''
        max_err = 0
        min_err_lesson = ''
        min_err = 999999
        last_true = False
        """For counting in statistics only finished lessons"""
        for table in ERROR_TABLES:
            sql_command = f"select * from {table} where telegram_id = {telegram_id};"
            df = self._get_df_from_db(sql_command)

            for col in df.columns:
                if "cleared" in col:
                    if bool(df[col][0]) is True:
                        count_true += 1
                        last_true = True
                    else:
                        last_true = False
                elif "id" not in col and last_true:
                    col_val = int(df[col][0])
                    count_err += col_val
                    if min_err > col_val:
                        min_err = col_val
                        min_err_lesson = col
                    if max_err < col_val:
                        max_err = col_val
                        max_err_lesson = col
        if min_err == 999999:
            min_err = 0
        return count_true, count_err, max_err_lesson, min_err_lesson

    def update_time_analytics(self):
        last_hour_full_date = datetime.datetime.now() - datetime.timedelta(hours=1)
        # last_day = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # last_week = datetime.datetime.now() - datetime.timedelta(days=7)
        # month_first_day = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        current_date = datetime.datetime.now().date()
        last_hour = int(last_hour_full_date.hour)

        sql_command = f"select exists(select 1 from time_analytics where date='{current_date}');"
        # FIXME something wrong with next sql request...
        try:
            current_date_exists = bool(read_sql(sql_command, self.conn).iloc[0][0])
        except:
            raise

        if not current_date_exists:
            self._set_data(INSERT_DATE_TIME_ANALYTICS.format(current_date))

        sql_command = f"select count(last_seen) from person where last_seen >= '{last_hour_full_date}';"
        result = int(read_sql(sql_command, self.conn).iloc[0][0])

        sql_command = f"update time_analytics set hour_{last_hour}={result} where date='{current_date}';"
        self._set_data(sql_command)
        return True

    def get_missing(self, minutes):
        last_hour_full_date = datetime.datetime.now() - datetime.timedelta(minutes=minutes)
        new_hour = last_hour_full_date - datetime.timedelta(minutes=60)

        print(last_hour_full_date, new_hour)
        sql_command = f"select telegram_id from person where last_seen <= '{last_hour_full_date}' " \
                      f"AND last_seen >='{new_hour}';"
        result = self._get_df_from_db(sql_command)['telegram_id'].tolist()
        return result

    def get_all_tables(self):
        ...

    def del_table(self, name) -> bool:
        ...

    def get_table_from_redis(self, table_name:str) -> dict:
        my_r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        redis_table = my_r.get(table_name)
        table = loads(redis_table)
        return table

    def cache_tables(self, mother_table_name: str = "bot_structure", specific_table: str = None) -> bool:
        sql = f'select * from {mother_table_name};'

        try:
            mother_table = self._get_df_from_db(sql)
        except psycopg2.Error:
            raise KeyError("There is no mother table in db")
        menu_base = dict()
        for value in mother_table.values:
            # we need to address to value[0]
            print(value[0])
            sql = f'select * from {value[0]};'
            try:
                table = self._get_df_from_db(sql)
            except Exception:
                # FIXME handle pandas.io.sql.DatabaseError
                continue
            table = table.to_dict()
            menu_base.update({value[0]: table})

        # now time to give it to redis
        my_r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        my_r.flushdb()
        with my_r.pipeline() as pipe:
            for h_id, menu in menu_base.items():
                pipe.set(h_id, dumps(menu))
            pipe.execute()
            
        return True
    """
    {'main_menu': {
        'buttons_text': {0: 'Курсы', 1: '!nl', 2: 'Статистика', 3: 'Команды', 4: '!nl', 5: 'О предлоге'},
        'buttons_callback': {0: '!table courses', 1: '', 2: '!table statistics', 3: '!table commands', 4: '',
                            5: '!table about'}, 'picture_id': {0: '', 1: '', 2: '', 3: '', 4: '', 5: ''},
        'gif_id': {0: 'CgACAgIAAxkBAAIERmDdvJER5fMQxUc4en7tS4V3hLCoAAJ0DwACJM7xSlk20q7_2sIyIAQ',
                1: 'CgACAgIAAxkBAAIESGDdvHpDAdSEDQ-7AdGl3t3IGzduAAJ1DwACJM7xSlADIm0Mn4zpIAQ',
                2: 'CgACAgIAAxkBAAIESmDdvHo-WFFYY-40qPSna2ChCtKQAAJ2DwACJM7xSkMKY5NGOEWUIAQ',
                3: 'CgACAgIAAxkBAAIETGDdvFCLe7Q5mpcCwkAgDWSu4l2lAAJ4DwACJM7xSt8DdSEjcXsjIAQ',
                4: 'CgACAgIAAxkBAAIETmDdvJIBZVMa6wXmsPUBZctMu8oPAAJ5DwACJM7xSg8PG4Qa6jdRIAQ',
                5: 'CgACAgIAAxkBAAIEUGDdvJqoRRj4KgfHNEga1Z1d9Pu_AAJ6DwACJM7xSlXgB01Tft28IAQ'}, 'text': {
            0: 'Добро пожаловать в «Предлог».\n\nВместе с нашим ботом вы пройдёте интересные истории, полные умных правил и современных примеров русского языка.\n\nДля навигации используйте меню.',
            1: 'Приветствуем вас в «Предлоге»!\n\nЗдесь русский язык преподаёт бот, а по адресу @prdlg развлечём вас рубриками нашего канала — присоединяйтесь!\n\nКлавиши меню ниже помогут вам сориентироваться.',
            2: 'Здравствуйте, рады видеть вас в «Предлоге».\n\nКурсы от нашего бота здорово помогут вашему великому и могучему, но для полного погружения в учёбу советуем в довесок подписаться на наш канал — @prdlg.\n\nНадоело это меню? Просто воспользуйтесь клавишами ниже, чтобы отправиться в следующее.',
            3: 'Привет вам от «Предлога».\n\nЧего тянуть кота за хвост? Давайте учиться!\n\nЧтобы отправиться за знаниями используйте меню.',
            4: 'Наконец-то вы с «Предлогом»!\n\nХватит ошибаться в резюме и на корпоративной почте, пора завязывать с безграмотными комментариями и постами, нужно утереть нос своим проблемам с русским языком!\n\nМеню вашего бота к вашим услугам.\x0c',
            5: 'Доброго времени суток.\n\nЕсли вы не знали, у «Предлога» есть не только бот, но и канал с кучей весёлых рубрик по русскому языку, подписывайтесь на @prdlg.\n\nДля дальнейшей работы с ботом используйте меню.'}}}
    """

    def make_table_from_csv(self, csv_file, file_name: str, mother_table_name: str = "bot_structure") -> bool:
        # внутри csv должна быть строка с именами колонок и строка с типом данных для каждой колонки
        table_name = file_name.replace('.csv', '')

        
        sql = f'drop table if exists {table_name};'
        self._set_data(sql)
        cursor = self.conn.cursor()

        with open(csv_file, 'r', encoding="UTF-8") as f:
            reader = csv.reader(f)
            # next(reader)  # Skip the header row.
            # Производим заполнение базы данных путем итерации по csv
            for my_iter, row in enumerate(reader):
                if my_iter == 0:
                    # first row should contain columns headers, it will create appropriate list
                    # also here we will create appropriate quantity of %s symbols to insert other rows
                    standard_insert = ''
                    for i in range(len(row) - 1):
                        standard_insert += '%s, '
                    standard_insert += '%s'
                    column_names = row
                elif my_iter == 1:
                    sql = f'create table {table_name}('
                    for i in range(len(row) - 1):
                        sql += f"{column_names[i]} {row[i]}, "
                    sql += f"{column_names[-1]} {row[-1]});"
                    try:
                        cursor.execute(sql)
                    except psycopg2.Error:
                        # если вторая строка не имеет валидных типов данных, то ставим каждый тип данных
                        # в varchar(255)
                        sql = f'create table {table_name}('
                        for i in range(len(row) - 1):
                            sql += f"{column_names[i]} varchar(255), "
                        sql += f"{column_names[-1]} varchar(255));"
                        cursor.execute(sql)
                        # а затем эту строку вставляем в заполненную базу
                        cursor.execute(
                            f"INSERT INTO {table_name} VALUES ({standard_insert})",
                            row
                        )
                else:
                    cursor.execute(
                        f"INSERT INTO {table_name} VALUES ({standard_insert})",
                        row
                    )
        if table_name != mother_table_name:
            # if table_name not bot_structure => we will insert date of
            # uodating this table to bot_structure a.k.a mother table
            # f"'{datetime.datetime.now()}'"
            # sql = f"INSERT INTO {mother_table_name} VALUES('{datetime.datetime.now()}') WHERE table_name={table_name};"
            sql = f"UPDATE {mother_table_name} SET update_time = '{datetime.datetime.now()}' WHERE table_name='{table_name}';"
            cursor.execute(sql)

            # cursor.copy_from(f, table_name, sep=',', null='')
        self.conn.commit()
        return True


if __name__ == '__main__':
    # base = BaseHandler('predlog', r"C:\Projects\predlog_the_bot\.env")
    a = time.perf_counter()
    # print(base.get_missing(1))
    # df = base.get_person_current_fields(telegram_id=388863828)
    print(time.perf_counter() - a)
    # print(df)
    # a = time.perf_counter()
    # df = base.check_person_exists(telegram_id=388863828)
    # print(time.perf_counter() - a)
    # print(df)
