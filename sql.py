import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2 import pool
import logging
from tenacity import retry
import tenacity
import sys


class psql:

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.log = logging.getLogger('sql')
        self.log.setLevel(logging.INFO)
        self.connection = psycopg2
        self.builder = psycopg2.sql
        try:
            self.connection = psycopg2.connect(
                host=kwargs['host'],
                port=kwargs['port'],
                database=kwargs['database'],
                user=kwargs['user'],
                password=kwargs['password']
            )
            self.cursor = self.connection.cursor()
            self.dict_cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except (Exception, psycopg2.Error) as psql_error:
            self.log.fatal(psql_error)
            print("Error while connecting to " + kwargs['host'], psql_error)
            exit(99)

    @retry(stop=(tenacity.stop_after_delay(300) | tenacity.stop_after_attempt(2)),
           wait=tenacity.wait_exponential(multiplier=1, min=4, max=10))
    def create_pool(self):
        try:
            psql.pool = psycopg2.pool.ThreadedConnectionPool(2, 25,
                                                             host=self.kwargs['host'],
                                                             port=self.kwargs['port'],
                                                             database=self.kwargs['database'],
                                                             user=self.kwargs['user'],
                                                             password=self.kwargs['password']
                                                             )
        except (Exception, psycopg2.Error) as psql_error:
            self.log.fatal(psql_error)
            print("Error while connecting to {0}".format(self.host), psql_error)
            raise ConnectionError("Error while connecting to {0}".format(self.host), psql_error)

    def commit(self):
        self.connection.commit()

    def close(self, commit=False):
        if commit is True:
            self.connection.commit()
        self.connection.close()

    @retry(stop=(tenacity.stop_after_delay(300) | tenacity.stop_after_attempt(2)),
           wait=tenacity.wait_exponential(multiplier=1, min=4, max=10))
    def insert(self, statement, payload):
        try:
            self.cursor.execute(statement, payload)
        except (Exception, psycopg2.Error) as sql_error:
            self.log.error(sql_error)
            sys.exit(99)
        if self.cursor.description is not None:
            return self.cursor.fetchall()

    @retry(stop=(tenacity.stop_after_delay(300) | tenacity.stop_after_attempt(2)),
           wait=tenacity.wait_exponential(multiplier=1, min=4, max=10))
    def query(self, statement, as_dict=False):
        if as_dict:
            cursor = self.dict_cursor
        else:
            cursor = self.cursor

        try:
            cursor.execute(statement)
        except (Exception, psycopg2.Error) as sql_error:
            self.log.error(sql_error)
            sys.exit(99)
        if cursor.description is not None:
            return cursor.fetchall()

    @retry(stop=(tenacity.stop_after_delay(300) | tenacity.stop_after_attempt(2)),
           wait=tenacity.wait_exponential(multiplier=1, min=4, max=10))
    def copy(self, statement, data):
        try:
            self.cursor.copy_expert(statement, data)
        except (Exception, psycopg2.Error) as sql_error:
            self.log.error(sql_error)
            sys.exit(99)

    def upsert(self, input_iterable, target_schema=None, target_table=None):
        for row in input_iterable:
            print(row)
        pass
        # # Prepare
        # statement_temp_ddl = sql.builder.SQL(
        #     "CREATE TEMPORARY TABLE IF NOT EXISTS {} AS SELECT * FROM {}.{} WITH NO DATA;").format(
        #     sql.builder.Identifier("tmp_{}".format(target_table)),
        #     sql.builder.Identifier(target_schema),
        #     sql.builder.Identifier(target_table)
        # )
        #
        # statement_copy_from = sql.builder.SQL("COPY {} FROM STDIN WITH CSV;").format(
        #     sql.builder.Identifier("tmp_{}".format(target_table))
        # )
        # statement_insert = sql.builder.SQL("""
        # INSERT INTO {}.{} SELECT * FROM {} ON CONFLICT (uuid) DO UPDATE SET
        # date = EXCLUDED.date,
        # id = EXCLUDED.id,
        # topic = EXCLUDED.topic,
        # host = EXCLUDED.host,
        # email = EXCLUDED.email,
        # user_type = EXCLUDED.user_type,
        # start_time = EXCLUDED.start_time,
        # end_time = EXCLUDED.end_time,
        # duration = EXCLUDED.duration,
        # participants = EXCLUDED.participants
        # """).format(
        #     sql.builder.Identifier(target_schema),
        #     sql.builder.Identifier(target_table),
        #     sql.builder.Identifier("tmp_{}".format(target_table))
        # )
        #
        # # Execute
        # input.seek(0)  # read from the beginning
        # sql.query(statement_temp_ddl)
        # sql.copy(statement_copy_from, input)
        # sql.query(statement_insert)
        # sql.commit()
        # pass
