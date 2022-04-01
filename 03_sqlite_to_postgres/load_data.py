import datetime
import sqlite3

import psycopg2
import uuid
from dataclasses import dataclass, field
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from contextlib import contextmanager

# В коде есть обработка ошибок записи и чтения.
# Переменные окружения
# all tables - загрузить соотношения
# загрузка дат



@dataclass
class FilmWork:
    title: str
    description: str
    file_path: str
    type: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    description: field(default_factory='')
    # created_at: str
    # modified_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    # created_at: str
    # modified_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    film_work_id: str
    genre_id: str
    # created_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


def create_tables_list():
    tables_list = list()
    tables_list.append('film_work')
    tables_list.append('genre')
    # tables_list.append('genre_film_work')
    tables_list.append('person')
    # tables_list.append('person_film_work')
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
            k += n
            print(' .join(('Таблица:', table, 'обработано:', str(k), 'строк')))
        else:
            break


def generate_list_objects(p_curs, table_name, data):
    if table_name == 'genre':
        data = generate_genres(data)
        save_data_to_table_genre(p_curs, table_name, data)
    elif table_name == 'person':
        data = generate_persons(data)
        save_data_to_table_person(p_curs, table_name, data)
    elif table_name == 'film_work':
        data = generate_films(data)
        save_data_to_table_film_work(p_curs, table_name, data)


def generate_genres(data):
    genres = list()
    for row in data:
        genre = Genre(name=row['name'],
                      description=row['description'],
                      # created_at=row['created_at'],
                      # modified_at=row['updated_at'],
                      id=row['id'])
        if genre.description is None:
            genre.description = ''
        genres.append(genre)
    return genres


def save_data_to_table_genre(p_curs: _connection.cursor,
                             table_name: str, data: list):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, name, description)
                        VALUES ('{id}', '{name}', '{description}')
                        '''.format(table_name=table_name, id=element.id, name=element.name,
                                   description=element.description)
        p_curs.execute(insert_query)


def generate_persons(data):
    persons = list()
    for row in data:
        person = Person(full_name=row['full_name'],
                      # created_at=row['created_at'],
                      # modified_at=row['updated_at'],
                      id=row['id'])
        person.full_name = person.full_name.replace("'", "''")
        persons.append(person)
    return persons


def save_data_to_table_person(p_curs, table_name, data):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, full_name)
                        VALUES ('{id}', '{full_name}')
                        '''.format(table_name=table_name, id=element.id,
                                   full_name=element.full_name,)
        p_curs.execute(insert_query)


def generate_films(data):
    films = list()
    for row in data:
        film = FilmWork(title=row['title'],
                         description=row['description'],
                      # creation_date=row['creation_date'],
                      # created_at=row['created_at'],
                      # modified_at=row['updated_at'],
                         file_path=row['file_path'],
                         rating=row['rating'],
                         type=row['type'],
                         id=row['id'])
        film.title = film.title.replace("'", "''")
        if film.description is None:
            film.description = ''
        else:
            film.description = film.description.replace("'", "''")
        if film.file_path is None:
            film.file_path = ''
        if film.rating is None:
            film.rating = 0
        else:
            film.rating=float(film.rating)
        films.append(film)

    return films


def save_data_to_table_film_work(p_curs: _connection.cursor,
                             table_name: str, data: list):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, title, description, file_path,
                        rating, type)
                        VALUES ('{id}', '{title}', '{description}', '{file_path}', 
                        '{rating}', '{type}')
                        '''.format(table_name=table_name, id=element.id, title=element.title,
                                   description=element.description, file_path=element.file_path,
                                   rating=element.rating, type=element.type)
        p_curs.execute(insert_query)



def save_data_to_table_genre_film_work(p_curs, table_name, data):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, name, description)
                        VALUES ('{id}', '{name}', '{description}')
                        '''.format(table_name=table_name, id=element.id, name=element.name,
                                   description=element.description)
        p_curs.execute(insert_query)


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


if __name__ == '__main__':
    # вынести в переменные окружения
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5432}
    db_path = 'db.sqlite'
    #   with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    with conn_context(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
