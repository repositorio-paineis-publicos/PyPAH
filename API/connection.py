import duckdb

from storage import configure_duckdb


def get_con():
    con = duckdb.connect()
    return configure_duckdb(con)