ERROR_TABLES = ('chapter_1_errors', 'chapter_2_errors')
CREATE_DB = f'create database {database_name};'
CREATE_TABLE_PERSON = """
create table person(
id serial primary key,
telegram_id integer UNIQUE,
name varchar(255),
stage varchar(255),
current_lesson_key varchar(255),
current_lesson_stage integer default 0,
current_answer varchar(255),
count_yo integer default 0,
last_seen timestamp);
"""

CREATE_TABLE = """
create table {}(
id serial primary key,
telegram_id integer references person(telegram_id) UNIQUE);"""
"""
with .format method u can create any table, that will refer to table person by telegram_id
"""

CREATE_TIME_ANALYTICS_2 = """
create table time_analytics(
max_in_hour integer,
was_in_last_hour integer,
max_in_day integer,
was_in_last_day integer,
max_in_week integer,
was_in_last_week integer,
max_in_month integer,
was_in_last_month integer);
"""
CREATE_TIME_ANALYTICS = """
create table time_analytics(
id serial primary key,
date date not null unique,
hour_0 integer default 0,
hour_1 integer default 0,
hour_2 integer default 0,
hour_3 integer default 0,
hour_4 integer default 0,
hour_5 integer default 0,
hour_6 integer default 0,
hour_7 integer default 0,
hour_8 integer default 0,
hour_9 integer default 0,
hour_10 integer default 0,
hour_11 integer default 0,
hour_12 integer default 0,
hour_13 integer default 0,
hour_14 integer default 0,
hour_15 integer default 0,
hour_16 integer default 0,
hour_17 integer default 0,
hour_18 integer default 0,
hour_19 integer default 0,
hour_20 integer default 0,
hour_21 integer default 0,
hour_22 integer default 0,
hour_23 integer default 0);
"""
INSERT_DATE_TIME_ANALYTICS = "insert into time_analytics(date) values('{}');"
SQL_GET_COUNT_YO_FROM_PERSON = """
select count_yo from person where telegram_id = '{}';
"""
"""
.format telegram_id is necessary
"""
SQL_GET_ERRORS_FROM_LESSON = """
select {} from {} where telegram_id = {};
"""
"""
.format lesson_key, table_name and telegram_id is necessary
"""