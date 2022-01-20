CREATE_METRICS_TABLE = """
create table metrics(
    id serial primary key,
    status_code integer,
    response_time_s float default 0.0,
    value float default -1.0,
    checker_time timestamp
);
"""

INSERT_ALL = """
insert into metrics (status_code, response_time_s, value, checker_time) values(
    {},
    {},
    {},
    '{}'
);
"""

INSERT = """
insert into metrics (status_code, checker_time) values(
    {},
    '{}'
);
"""