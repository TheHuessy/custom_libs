import os
import pandas as pd
import yaml
import sqlalchemy

class SQLUtils:

    def __init__(self, database_name: str):
        self.db_name = database_name

    def select_query_builder(self, table_name: str, cols: str = None, limit_value: str = None, where_col: str = None, where_value: str = None, where_equal: str = None):
        """Funtion that takes in arguments and builds the appropriate SQL query."""

        limit_clause = "LIMIT {}".format(limit_value) if limit_value else None

        cols = ", ".join(cols) if cols else "*"

        where_clause = "WHERE {} {} '{}'".format(where_col, where_equal, where_value) if where_col else None

        statement = ["SELECT {} FROM {}".format(cols, table_name)]
        statement += [where_clause] if where_clause else ""
        statement += [limit_clause] if limit_clause else ""

        output_statement = " ".join(statement)

        return output_statement

    def update_query_builder(self, table_name: str, update_col: str, update_value: str, where_col: str = None, where_value: str = None, where_equal: str = None):
        """Funtion that takes in arguments and builds the appropriate SQL UPDATE statement."""

        where_clause = "WHERE {} {} '{}'".format(where_col, where_equal, where_value) if where_col else None
        statement = ["UPDATE {} SET {} = {}".format(table_name, update_col, update_value)]
        statement += [where_clause] if where_clause else ""

        output_statement = " ".join(statement)

        return output_statement

    def get(self, table_name: str, cols: str = None, limit_value: str = None, where_col: str = None, where_value: str = None, where_equal: str = None, verbose: bool = False):
        """Funtion that takes in arguments and executes a SELECT query."""

        query = self.select_query_builder(table_name=table_name, cols=cols, limit_value=limit_value, where_col=where_col, where_value=where_value, where_equal=where_equal)

        if verbose:
            print(query)

        result = self.execute(query)

        return result

    def update(self, table_name: str, update_col: str, update_value: str, where_equal: str = None, where_col: str = None, where_value: str = None, verbose: bool = False):
        """Funtion that takes in arguments and executes an UPDATE statement."""

        query = self.update_query_builder(table_name=table_name, update_col=update_col, update_value=update_value, where_col=where_col, where_value=where_value, where_equal=where_equal)

        if verbose:
            print(query)

        result = self.execute(query)

        return result

    def execute(self, statement: str):
        """Funtion that executes a provided SQL statement on the object database name."""

        with SQLEngine(self.db_name) as sql_engine:
            if "SELECT" in statement.upper():
                return pd.read_sql(statement, sql_engine)
            else:
                return sql_engine.execute(statement)

class SQLEngine:

    def conn_string_gen(self):
        """Funtion that builds a postgres connection string based on initailized attributes."""

        return "postgresql+psycopg2://{}:{}@{}/{}".format(self.un, self.pw, self.host, self.db_name)

    def __init__(self, database: str):
        with open(os.environ['CREDS_PATH']) as file:
            self.creds = yaml.full_load(file)

        self.db_name = database
        self.un = self.creds['pg_user']
        self.pw = self.creds['pg_pw']
        self.host = self.creds['pg_host']


    def __enter__(self):
        """Context Manager: enter method. Prepares the engine object."""
        conn_string = self.conn_string_gen()
        self.engine = sqlalchemy.create_engine(conn_string)

        return self.engine

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context Manager: exit method. Closes the connection made on enter."""
        if self.engine:
            self.engine.dispose()

