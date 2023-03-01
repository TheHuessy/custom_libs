# from custom_libs import SQLEngine
import os
import pandas as pd
import yaml
import sqlalchemy

class SQLUtils:

    def __init__(self, database_name: str):
        # with open(os.environ['CREDS_PATH']) as file:
        #     self.creds = yaml.full_load(file)
        self.db_name = database_name

    def select_query_builder(self, table_name, cols=None, limit_value=None, where_col=None, where_value=None, where_equal=None):

        limit_clause = "LIMIT {}".format(limit_value) if limit_value else None

        cols = ", ".join(cols) if cols else "*"

        where_clause = "WHERE {} {} '{}'".format(where_col, where_equal, where_value) if where_col else None

        statement = ["SELECT {} FROM {}".format(cols, table_name)]
        statement += [where_clause] if where_clause else ""
        statement += [limit_clause] if limit_clause else ""

        output_statement = " ".join(statement)

        return(output_statement)

    def update_query_builder(self, table_name, update_col, update_value, where_col=None, where_value=None, where_equal=None):
        where_clause = "WHERE {} {} '{}'".format(where_col, where_equal, where_value) if where_col else None
        statement = ["UPDATE {} SET {} = {}".format(table_name, update_col, update_value)]
        statement += [where_clause] if where_clause else ""

        output_statement = " ".join(statement)

        return(output_statement)

    def get(self, table_name, cols=None, limit_value=None, where_col=None, where_value=None, where_equal=None):
        query = self.select_query_builder(table_name=table_name, cols=cols, limit_value=limit_value, where_col=where_col, where_value=where_value, where_equal=where_equal)

        print(query)

        result = self.execute(query)

        return(result)

    def update(self, table_name, update_col, update_value, where_equal=None, where_col=None, where_value=None):
        query = self.update_query_builder(table_name=table_name, update_col=update_col, update_value=update_value, where_col=where_col, where_value=where_value, where_equal=where_equal)

        print(query)

        result = self.execute(query)

        return(result)

    def execute(self, statement: str):
        with SQLEngine.SQLEngine(self.db_name) as sql_engine:
            if "SELECT" in statement.upper():
                return(pd.read_sql(statement, sql_engine))
            else:
                return(sql_engine.execute(statement))

class SQLEngine:

    # def conn_string_gen(self, db_name: str, user_name: str, pw: str, host: str):
    def conn_string_gen(self):
        # output_conn_string = "postgresql+psycopg2://{}:{}@{}/{}".format(user_name, pw,db_name, host)
        output_conn_string = "postgresql+psycopg2://{}:{}@{}/{}".format(self.un, self.pw, self.db_name, self.host)
        return(output_conn_string)

    def __init__(self, database: str):
        with open(os.environ['CREDS_PATH']) as file:
            self.creds = yaml.full_load(file)

        self.db_name = database
        self.un = self.creds['pg_user']
        self.pw = self.creds['pg_pw']
        self.host = self.creds['pg_host']


    def __enter__(self):
        # conn_string = self.conn_string_gen(self.creds['pg_user'], self.creds['pg_pw'], self.creds['pg_host'])
        conn_string = self.conn_string_gen()
        self.engine = sqlalchemy.create_engine(conn_string)

        return(self.engine)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.engine:
            self.engine.dispose()

