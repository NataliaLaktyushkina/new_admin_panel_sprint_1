import unittest
import os
import psycopg2
import sqlite3
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from contextlib import contextmanager
from dateutil.parser import parse

#  utilities:
# env

class TestLoadingData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        env_var = get_environment_var()

        dsl = {'dbname': env_var['db_name'],
               'user': env_var['user'],
               'password': env_var['password'],
               'host': env_var['host'],
               'port': env_var['port']}

        db_path = os.path.join(os.getcwd(), env_var['db_path'][0])

        cls.sqlite_conn = sqlite3.connect(db_path)
        cls.sqlite_conn.row_factory = sqlite3.Row
        cls.sqlite_curs = cls.sqlite_conn.cursor()

        cls.pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        cls.p_curs = cls.pg_conn.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.sqlite_conn.close()
        cls.pg_conn.close()

    def compare(self, table_name):

        sqlite_query_text = 'SELECT * FROM ' + table_name
        psql_query_text = 'SELECT * FROM content.' + table_name

        self.sqlite_curs.execute(sqlite_query_text)
        sqlite_rows = self.sqlite_curs.fetchall()

        self.p_curs.execute(psql_query_text)
        p_rows = self.p_curs.fetchall();

        self.assertEqual(len(sqlite_rows), len(p_rows))

    def test_number_of_records_genre(self):
        table_name = 'genre'
        self.compare(table_name)

    def test_number_of_records_person(self):
        table_name = 'person'
        self.compare(table_name)

    def test_number_of_records_film_work(self):
        table_name = 'film_work'
        self.compare(table_name)

    def test_number_of_records_person_film_work(self):
        table_name = 'person_film_work'
        self.compare(table_name)

    def test_number_of_records_genre_film_work(self):
        table_name = 'genre_film_Work'
        self.compare(table_name)

    def test_table_content_genre(self):

        table_name = 'genre'
        # получить порцию данных их sqlite
        query_text = "SELECT * FROM " + table_name + ";"
        self.sqlite_curs.execute(query_text)
        n = 100
        while True:
            rows = self.sqlite_curs.fetchmany(n)
            if rows:
                for row in rows:
                    psql_query_text = '''
                        SELECT * FROM content.genre
                        WHERE id = %(id)s
                            AND name = %(name)s
                            AND  description = %(description)s
                            AND  created_at = %(created_at)s
                            AND  updated_at = %(updated_at)s
                            '''

                    self.p_curs.execute(psql_query_text,
                                        {'id': row['id'],
                                         'name': row['name'],
                                         'description': (row['description'] if row['description'] is not None else ''),
                                         'created_at': parse(row['created_at']),
                                         'updated_at': parse(row['updated_at'])})
                    p_rows = self.p_curs.fetchall();
                    self.assertEqual(1, len(p_rows))
            else:
                break

        # есди количество записей = 1, то тест пройден
        # сравнить массив данных?


def get_environment_var():
    os.chdir('../..')
    dotenv_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path)
    env_var = {}
    env_var['db_name'] = os.environ.get('DB_NAME')
    env_var['user'] = os.environ.get('USER_APP')
    env_var['password'] = os.environ.get('PASSWORD')
    env_var['host'] = os.environ.get('HOST')
    env_var['port'] = os.environ.get('PORT')
    env_var['db_path'] = os.environ.get('DB_PATH'),

    return env_var


if __name__ == "__main__":
    unittest.main()
