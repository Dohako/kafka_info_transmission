CREATE_METRICS_TABLE = """
create table metrics(
    id serial primary key,
    status_code integer,
    response_time_s float default 0.0,
    value float default -1.0,
    checker_time timestamp,
    local_time timestamp
);
"""
"""
SQL query to create table metrics from scratch.
Needed if it doesn't exists yet.
"""

INSERT_ALL = """
insert into metrics (status_code, response_time_s, value, checker_time, local_time) values(
    {},
    {},
    {},
    '{}',
    '{}'
);
"""
"""
SQL query to insert all possible data.
Data order: status_code, response_time_s, value, checker_time, local_time
"""

INSERT = """
insert into metrics (status_code, checker_time, local_time) values(
    {},
    '{}',
    '{}'
);
"""
"""
SQL query to insert only status_code and checker time if other data is failed to obtain or zero.
Data order: status_code, checker_time, local_time
"""