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

# В коде есть обработка ошибок записи и чтения.
# провера дат, проверка дублей


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
    query_text = "SELECT * FROM " + table + ";"
    curs.execute(query_text)
    p_curs = pg_conn.cursor()
    truncate_query = ' '.join(('TRUNCATE content.', table, 'CASCADE; '))
    p_curs.execute(truncate_query)
    n = 100
    k = 0
    while True:
        rows = curs.fetchmany(n)
        if rows:
            generate_list_objects(p_curs, table, rows)
            k += len(rows)
            print(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
        else:
            break


def generate_list_objects(p_curs, table_name, data):
    if table_name == 'genre':
        data = generate_genres(data)
        for element in data:
            save_data_to_table_genre(p_curs, element)
    elif table_name == 'person':
        data = generate_persons(data)
        for element in data:
            save_data_to_table_person(p_curs, element)
    elif table_name == 'film_work':
        data = generate_films(data)
        for element in data:
            save_data_to_table_film_work(p_curs, element)
    elif table_name == 'genre_film_work':
        data = generate_genre_film_work(data)
        for element in data:
            save_data_to_table_genre_film_work(p_curs, element)
    elif table_name == 'person_film_work':
        data = generate_person_film_work(data)
        for element in data:
            save_data_to_table_person_film_work(p_curs, element)


def generate_genres(data):
    genres = list()
    for row in data:
        genre = Genre(name=row['name'],
                      description=row['description'],
                      created_at=row['created_at'],
                      modified_at=row['updated_at'],
                      id=row['id'])
        if genre.description is None:
            genre.description = ''
        genre.created_at = parse(genre.created_at)
        genre.modified_at = parse(genre.modified_at)
        genres.append(genre)
    return genres


def save_data_to_table_genre(p_curs: _connection.cursor, genre: Genre):

    insert_query = '''
        INSERT INTO content.genre
            (id, name, description, created_at, updated_at)
        VALUES (%(id)s, %(name)s, %(description)s, %(created_at)s, %(modified_at)s
        )'''

    p_curs.execute(insert_query, {'id': genre.id,
                                  'name': genre.name,
                                  'description': genre.description,
                                  'created_at': genre.created_at,
                                  'modified_at': genre.modified_at})


def generate_persons(data):
    persons = list()
    for row in data:
        person = Person(full_name=row['full_name'],
                        created_at=row['created_at'],
                        modified_at=row['updated_at'],
                        id=row['id'])
        person.full_name = person.full_name.replace("'", "''")
        person.created_at = parse(person.created_at)
        person.modified_at = parse(person.modified_at)
        persons.append(person)
    return persons


def save_data_to_table_person(p_curs: _connection.cursor, person: Person):

    insert_query = '''
        INSERT INTO content.person
            (id, full_name, created_at, updated_at)
        VALUES ( 
            %(id)s, %(full_name)s, %(created_at)s, %(modified_at)s
            )'''

    p_curs.execute(insert_query, {'id': person.id,
                                  'full_name': person.full_name,
                                  'created_at': person.created_at,
                                  'modified_at': person.modified_at})


def generate_films(data):

    films = list()
    for row in data:
        film = FilmWork(title=row['title'],
                        description=row['description'],
                        creation_date=row['creation_date'],
                        created_at=row['created_at'],
                        modified_at=row['updated_at'],
                        file_path=row['file_path'],
                        rating=row['rating'],
                        type=row['type'],
                        id=row['id'])

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

        films.append(film)

    return films


def save_data_to_table_film_work(p_curs: _connection.cursor, film: FilmWork):

    insert_query = '''
        INSERT INTO content.film_work 
            (id, title, description, file_path,
            rating, type, creation_date, created_at, updated_at )
        VALUES (
            %(id)s, %(title)s, %(description)s, %(file_path)s, %(rating)s, 
            %(type)s, %(creation_date)s, %(created_at)s, %(modified_at)s
            )'''
    p_curs.execute(insert_query, {'id': film.id,
                                  'title': film.title,
                                  'description': film.description,
                                  'file_path': film.file_path,
                                  'rating': film.rating,
                                  'type': film.type,
                                  'creation_date': film.creation_date,
                                  'created_at': film.created_at,
                                  'modified_at': film.modified_at})


def generate_genre_film_work(data):
    relations_genre_film = list()
    for row in data:
        genre_film = GenreFilmWork(film_work_id=row['film_work_id'],
                                   genre_id=row['genre_id'],
                                   created_at=row['created_at'],
                                   id=row['id'])

        genre_film.created_at = parse(genre_film.created_at)

        relations_genre_film.append(genre_film)

    return relations_genre_film


def save_data_to_table_genre_film_work(p_curs: _connection.cursor, genre_film: GenreFilmWork):

    insert_query = '''
        INSERT INTO content.genre_film_work
            (id, film_work_id, genre_id, created)
        VALUES (%(id)s, %(film_work_id)s, %(genre_id)s, %(created_at)s
            )'''

    p_curs.execute(insert_query, {'id': genre_film.id,
                                  'film_work_id': genre_film.film_work_id,
                                  'genre_id': genre_film.genre_id,
                                  'created_at': genre_film.created_at})


def generate_person_film_work(data):

    relations_person_film = list()
    for row in data:
        person_film = PersonFilmWork(film_work_id=row['film_work_id'],
                                     person_id=row['person_id'],
                                     role=row['role'],
                                     created_at=row['created_at'],
                                     id=row['id'])

        person_film.created_at = parse(person_film.created_at)
        relations_person_film.append(person_film)

    return relations_person_film


def save_data_to_table_person_film_work(p_curs: _connection.cursor, person_film: PersonFilmWork):

    insert_query = '''
        INSERT INTO content.person_film_work
            (id, film_work_id, person_id, role, created)
        VALUES (%(id)s, %(film_work_id)s, %(person_id)s, %(role)s, %(created_at)s
            )'''
    p_curs.execute(insert_query, {'id': person_film.id,
                                  'film_work_id': person_film.film_work_id,
                                  'person_id': person_film.person_id,
                                  'role': person_film.role,
                                  'created_at': person_film.created_at})


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    curs = connection.cursor()
    tables_list = create_tables_list()

    for table_name in tables_list:
        # data = get_data_from_table(curs, table_name)
        get_data_from_table(curs, pg_conn, table_name)
        # save_table_to_postgres(pg_conn, table_name, data)


@contextmanager
def conn_context(db_path: str):
    # Устанавливаем соединение с БД
    conn = sqlite3.connect(db_path)
    # По-умолчанию SQLite возвращает строки в виде кортежа значений.
    # Эта строка указывает, что данные должны быть в формате "ключ-значение"
    conn.row_factory = sqlite3.Row
    yield conn
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
