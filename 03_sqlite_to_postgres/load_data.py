import datetime
import sqlite3
import os
import psycopg2

import uuid
from dateutil.parser import parse
from dataclasses import dataclass, field
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from dotenv import load_dotenv

PAGE_SIZE = 1000


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: str
    created_at: str
    modified_at: str
    file_path: str
    type: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:

    name: str
    description: field(default_factory='')
    created_at: str
    modified_at: str

    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created_at: str
    modified_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    film_work_id: str
    genre_id: str
    created_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork:
    film_work_id: str
    person_id: str
    role: str
    created_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


def create_tables_list():

    tables_list = list()
    tables_list.append('film_work')
    tables_list.append('genre')
    tables_list.append('genre_film_work')
    tables_list.append('person')
    tables_list.append('person_film_work')

    return tables_list


def get_data_from_table(curs, pg_conn, table):

    try:
        with pg_conn.cursor() as p_curs:
        # p_curs = pg_conn.cursor()
            truncate_query = ' '.join(('TRUNCATE content.', table, 'CASCADE; '))
            p_curs.execute(truncate_query)

    except psycopg2.Error as error:
        print('Ошибка сокращения таблицы ', table, error)

    if table == 'genre':
        try:
            query_text = "SELECT name, description, created_at, updated_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                genres = [Genre(*row) for row in curs.fetchall()]
                if len(genres):
                    data = modify_genres(genres)
                    save_data_to_table_genre(pg_conn, data)
                    k += len(genres)
                    print(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            print('Ошибка чтения данных, таблица ', table, error)
    elif table == 'person':
        try:
            query_text = "SELECT full_name, created_at, updated_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                persons = [Person(*row) for row in curs.fetchall()]
                if len(persons):
                    data = modify_persons(persons)
                    save_data_to_table_person(pg_conn, data)
                    k += len(persons)
                    print(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            print('Ошибка чтения данных, таблица ', table, error)
    elif table == 'film_work':
        try:
            query_text = '''
                SELECT 
                    title, description, creation_date, created_at, updated_at, file_path, type, rating, id
                FROM ''' + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                films = [FilmWork(*row) for row in curs.fetchall()]
                if len(films):
                    data = modify_films(films)
                    save_data_to_table_film_work(pg_conn, data)
                    k += len(films)
                    print(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            print('Ошибка чтения данных, таблица ', table, error)
    elif table == 'genre_film_work':
        try:
            query_text = "SELECT film_work_id, genre_id, created_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                genre_films = [GenreFilmWork(*row) for row in curs.fetchall()]
                if len(genre_films):
                    data = modify_genre_films(genre_films)
                    save_data_to_table_genre_film_work(pg_conn, data)
                    k += len(genre_films)
                    print(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            print('Ошибка чтения данных, таблица ', table, error)
    elif table == 'person_film_work':
        try:
            query_text = "SELECT film_work_id, person_id, role, created_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                person_films = [PersonFilmWork(*row) for row in curs.fetchall()]
                if len(person_films):
                    data = modify_person_films(person_films)
                    save_data_to_table_person_film_work(pg_conn, data)
                    k += len(person_films)
                    print(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            print('Ошибка чтения данных, таблица ', table, error)


def modify_genres(genres):

    for genre in genres:
        if genre.description is None:
            genre.description = ''
        if genre.created_at is not None:
            genre.created_at = parse(genre.created_at)
        if genre.modified_at is not None:
            genre.modified_at = parse(genre.modified_at)
    return genres


def save_data_to_table_genre(pg_conn: _connection, genres: Genre):

    insert_query = '''
        INSERT INTO content.genre
            (id, name, description, created_at, updated_at)
        VALUES (%(id)s, %(name)s, %(description)s, %(created_at)s, %(modified_at)s)
       
        ON CONFLICT (id)
            DO UPDATE SET  
                (name, description, created_at, updated_at) =  
                (EXCLUDED.name, EXCLUDED.description, EXCLUDED.created_at, EXCLUDED.updated_at) 
        '''

    insert_data = []
    for genre in genres:
        insert_data.append({'id': genre.id,
                            'name': genre.name,
                            'description': genre.description,
                            'created_at': genre.created_at,
                            'modified_at': genre.modified_at})
    try:
        with pg_conn.cursor() as p_curs:
            psycopg2.extras.execute_batch(p_curs, insert_query, insert_data, page_size=PAGE_SIZE)

    except psycopg2.Error as error:
        print('Ошибка записи данных в таблицу genre', error)


def modify_persons(persons):
    for person in persons:
        person.full_name = person.full_name.replace("'", "''")
        if person.created_at is not None:
            person.created_at = parse(person.created_at)
        if person.modified_at is not None:
            person.modified_at = parse(person.modified_at)
    return persons


def save_data_to_table_person(pg_conn: _connection, persons: Person):

    insert_query = '''
        INSERT INTO content.person
            (id, full_name, created_at, updated_at)
        VALUES ( 
            %(id)s, %(full_name)s, %(created_at)s, %(modified_at)s
            )
        ON CONFLICT (id)
            DO UPDATE SET  
                (full_name, created_at, updated_at) =  
                (EXCLUDED.full_name, EXCLUDED.created_at, EXCLUDED.updated_at) 
        '''

    insert_data = []
    for person in persons:
        insert_data.append({'id': person.id,
                            'full_name': person.full_name,
                            'created_at': person.created_at,
                            'modified_at': person.modified_at})
    try:
        with pg_conn.cursor() as p_curs:
            psycopg2.extras.execute_batch(p_curs, insert_query, insert_data, page_size=PAGE_SIZE)

    except psycopg2.Error as error:
        print('Ошибка записи данных в таблицу person', error)


def modify_films(films):

    for film in films:
        film.title = film.title.replace("'", "''")

        if film.description is not None:
            film.description = film.description.replace("'", "''")

        if film.creation_date is not None:
            film.creation_date = parse(film.creation_date)

        if film.created_at is not None:
            film.created_at = parse(film.created_at)
        else:
            film.created_at = datetime.datetime.now()

        if film.modified_at is not None:
            film.modified_at = parse(film.modified_at)
        else:
            film.modified_at = datetime.datetime.now()

    return films


def save_data_to_table_film_work(pg_conn: _connection, films: FilmWork):

    insert_query = '''
        INSERT INTO content.film_work 
            (id, title, description, file_path,
            rating, type, creation_date, created_at, updated_at )
        VALUES (
            %(id)s, %(title)s, %(description)s, %(file_path)s, %(rating)s, 
            %(type)s, %(creation_date)s, %(created_at)s, %(modified_at)s
            )
        ON CONFLICT (id)
            DO UPDATE SET  
                (title, description, file_path,
            rating, type, creation_date, created_at, updated_at) =  
                (EXCLUDED.title, EXCLUDED.description, EXCLUDED.file_path,
            EXCLUDED.rating, EXCLUDED.type, EXCLUDED.creation_date, 
            EXCLUDED.created_at, EXCLUDED.updated_at ) 
        '''

    insert_data = []
    for film in films:
        insert_data.append({'id': film.id,
                            'title': film.title,
                            'description': film.description,
                            'file_path': film.file_path,
                            'rating': film.rating,
                            'type': film.type,
                            'creation_date': film.creation_date,
                            'created_at': film.created_at,
                            'modified_at': film.modified_at})
    try:
        with pg_conn.cursor() as p_curs:
            psycopg2.extras.execute_batch(p_curs, insert_query, insert_data, page_size=PAGE_SIZE)

    except psycopg2.Error as error:
        print('Ошибка записи данных в таблицу film_work', error)


def modify_genre_films(genre_films):
    for genre_film in genre_films:
        if genre_film.created_at is not None:
            genre_film.created_at = parse(genre_film.created_at)

    return genre_films


def save_data_to_table_genre_film_work(pg_conn: _connection, genre_films: GenreFilmWork):

    insert_query = '''
        INSERT INTO content.genre_film_work
            (id, film_work_id, genre_id, created)
        VALUES (%(id)s, %(film_work_id)s, %(genre_id)s, %(created_at)s
            )
        ON CONFLICT (id)
            DO UPDATE SET  
                (film_work_id, genre_id, created) =  
                (EXCLUDED.film_work_id, EXCLUDED.genre_id, EXCLUDED.created)
            '''
    insert_data = []
    for genre_film in genre_films:
        insert_data.append({'id': genre_film.id,
                            'film_work_id': genre_film.film_work_id,
                            'genre_id': genre_film.genre_id,
                            'created_at': genre_film.created_at})
    try:
        with pg_conn.cursor() as p_curs:
            psycopg2.extras.execute_batch(p_curs, insert_query, insert_data, page_size=PAGE_SIZE)

    except psycopg2.Error as error:
        print('Ошибка записи данных в таблицу genre_film_work', error)


def modify_person_films(person_films):

    for person_film in person_films:
        if person_film.created_at is not None:
            person_film.created_at = parse(person_film.created_at)

    return person_films


def save_data_to_table_person_film_work(pg_conn: _connection, person_films: PersonFilmWork):

    insert_query = '''
        INSERT INTO content.person_film_work
            (id, film_work_id, person_id, role, created)
        VALUES (%(id)s, %(film_work_id)s, %(person_id)s, %(role)s, %(created_at)s)
        ON CONFLICT (id)
            DO UPDATE SET  
                (film_work_id, person_id, role, created) =  
                (EXCLUDED.film_work_id, EXCLUDED.person_id, EXCLUDED.role, EXCLUDED.created)           
        '''

    insert_data = []
    for person_film in person_films:
        insert_data.append({'id': person_film.id,
                            'film_work_id': person_film.film_work_id,
                            'person_id': person_film.person_id,
                            'role': person_film.role,
                            'created_at': person_film.created_at})
    try:
        with pg_conn.cursor() as p_curs:
            psycopg2.extras.execute_batch(p_curs, insert_query, insert_data, page_size=PAGE_SIZE)

    except psycopg2.Error as error:
        print('Ошибка записи данных в таблицу person_film_work', error)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    curs = connection.cursor()
    tables_list = create_tables_list()

    for table_name in tables_list:
        get_data_from_table(curs, pg_conn, table_name)


@contextmanager
def conn_context(db_path: str):
    # Устанавливаем соединение с БД
    try:
        conn = sqlite3.connect(db_path)
        # По-умолчанию SQLite возвращает строки в виде кортежа значений.
        # Эта строка указывает, что данные должны быть в формате "ключ-значение"
        conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as error:
        print('Ошибка соединения с sqlite', error)
    finally:
        if conn:
            conn.close()


def get_enviroment_var():
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    env_var = {}
    env_var['db_name'] = os.environ.get('DB_NAME')
    env_var['user'] = os.environ.get('USER_APP')
    env_var['password'] = os.environ.get('PASSWORD')
    env_var['host'] = os.environ.get('HOST')
    env_var['port'] = os.environ.get('PORT')
    env_var['db_path'] = os.environ.get('DB_PATH'),

    return env_var


if __name__ == '__main__':
    env_var = get_enviroment_var()

    dsl = {'dbname': env_var['db_name'],
           'user': env_var['user'],
           'password': env_var['password'],
           'host': env_var['host'],
           'port': env_var['port']}

    db_path = env_var['db_path']

    with conn_context(db_path[0]) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
    pg_conn.close()