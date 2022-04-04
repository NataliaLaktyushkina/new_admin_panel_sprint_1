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

        query_text = "SELECT * FROM genre"
        self.sqlite_curs.execute(query_text)

        while True:
            rows = self.sqlite_curs.fetchall()
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

    def test_table_content_person(self):

        query_text = "SELECT * FROM person"
        self.sqlite_curs.execute(query_text)

        while True:
            rows = self.sqlite_curs.fetchall()
            if rows:
                for row in rows:
                    psql_query_text = '''
                           SELECT * FROM content.person
                           WHERE id = %(id)s
                               AND full_name = %(full_name)s
                               AND  created_at = %(created_at)s
                               AND  updated_at = %(updated_at)s
                               '''

                    self.p_curs.execute(psql_query_text,
                                        {'id': row['id'],
                                         'full_name': row['full_name'].replace("'", "''"),
                                         'created_at': parse(row['created_at']),
                                         'updated_at': parse(row['updated_at'])})

                    p_rows = self.p_curs.fetchall();
                    self.assertEqual(1, len(p_rows), msg='full_name = ' + row['full_name'])
            else:
                break

    def test_table_content_films(self):

        query_text = "SELECT * FROM film_work"
        self.sqlite_curs.execute(query_text)

        while True:
            rows = self.sqlite_curs.fetchall()
            if rows:
                for row in rows:
                    psql_query_text = '''
                           SELECT * FROM content.film_work
                           WHERE id = %(id)s
                               AND title = %(title)s
                               AND type = %(type)s
                               AND created_at = %(created_at)s
                               AND updated_at = %(updated_at)s'''

                    if row['rating'] is None:
                        psql_query_text = psql_query_text + ' AND rating is NULL'
                    else:
                        psql_query_text = psql_query_text + ' AND rating = %(rating)s'
                    if row['description'] is None:
                        psql_query_text  = psql_query_text + ' AND description IS NULL'
                    else:
                        psql_query_text = psql_query_text + ' AND description = %(description)s'
                    if row['creation_date'] is None:
                        psql_query_text = psql_query_text + ' AND creation_date IS NULL'
                    else:
                        psql_query_text = psql_query_text + ' AND creation_date = %(creation_date)s'
                    if row['file_path'] is None:
                        psql_query_text = psql_query_text + ' AND file_path IS NULL'
                    else:
                        psql_query_text = psql_query_text + ' AND file_path = % (file_path)s'

                    self.p_curs.execute(psql_query_text,
                                        {'id': row['id'],
                                         'title': row['title'].replace("'", "''"),
                                         'description': (row['description'] if  row['description'] is None else row['description'].replace("'", "''")),
                                         'creation_date': row['creation_date'],
                                         'rating': row['rating'],
                                         'type': row['type'],
                                         'file_path': row['file_path'],
                                         'created_at': ('' if row['created_at'] is None else parse(row['created_at'])),
                                         'updated_at': ('' if row['updated_at'] is None else parse(row['updated_at']))})

                    p_rows = self.p_curs.fetchall();
                    self.assertEqual(1, len(p_rows), msg='title = ' + row['title'])
            else:
                break

    def test_table_content_genre_film(self):

        query_text = "SELECT * FROM genre_film_work"
        self.sqlite_curs.execute(query_text)

        while True:
            rows = self.sqlite_curs.fetchall()
            if rows:
                for row in rows:
                    psql_query_text = '''
                           SELECT * FROM content.genre_film_work
                           WHERE id = %(id)s
                               AND genre_id = %(genre_id)s
                               AND  film_work_id = %(film_work_id)s
                               AND  created = %(created)s
                               '''

                    self.p_curs.execute(psql_query_text,
                                        {'id': row['id'],
                                         'genre_id': row['genre_id'],
                                         'film_work_id': row['film_work_id'],
                                         'created': parse(row['created_at'])})

                    p_rows = self.p_curs.fetchall();
                    self.assertEqual(1, len(p_rows), msg='id = ' + row['id'])
            else:
                break

    def test_table_content_person_film(self):

        query_text = "SELECT * FROM person_film_work"
        self.sqlite_curs.execute(query_text)

        while True:
            rows = self.sqlite_curs.fetchall()
            if rows:
                for row in rows:
                    psql_query_text = '''
                           SELECT * FROM content.person_film_work
                           WHERE id = %(id)s
                               AND film_work_id = %(film_work_id)s
                               AND person_id = %(person_id)s
                               AND role = %(role)s
                               AND created = %(created)s
                               '''

                    self.p_curs.execute(psql_query_text,
                                        {'id': row['id'],
                                         'film_work_id': row['film_work_id'],
                                         'person_id': row['person_id'],
                                         'role': row['role'],
                                         'created': parse(row['created_at'])})

                    p_rows = self.p_curs.fetchall();
                    self.assertEqual(1, len(p_rows), msg='id = ' + row['id'])
            else:
                break


def get_environment_var():
    os.chdir('../..')
    dotenv_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path)
    env_var = {'db_name': os.environ.get('DB_NAME'), 'user': os.environ.get('USER_APP'),
               'password': os.environ.get('PASSWORD'), 'host': os.environ.get('HOST'), 'port': os.environ.get('PORT'),
               'db_path': (os.environ.get('DB_PATH'),)}

    return env_var


if __name__ == "__main__":
    unittest.main()
