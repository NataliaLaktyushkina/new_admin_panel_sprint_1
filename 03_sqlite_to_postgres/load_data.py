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
# Загружайте данные пачками по n записей.



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


def create_tables_list():
    tables_list = list()
    # tables_list.append('film_work')
    tables_list.append('genre')
    # tables_list.append('genre_film_work')
    #tables_list.append('person')
    # tables_list.append('person_film_work')
    return tables_list


def get_data_from_table(curs, pg_conn, table):
    query_text = "SELECT * FROM " + table + ";"
    curs.execute(query_text)
    # data = curs.fetchall()
    # print('Total rows in table ', table, ':', len(data))
    p_curs = pg_conn.cursor()
    truncate_query = ' '.join(('TRUNCATE content.', table, 'CASCADE; '))
    p_curs.execute(truncate_query)
    n = 10
    while True:
        rows = curs.fetchmany(n)
        if rows:
            data = generate_list_objects(table, rows)
            save_table_to_postgres(p_curs, table, data)
        else:
            break


def generate_list_objects(table_name, data):
    if table_name == 'genre':
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


def save_table_to_postgres(p_curs: _connection.cursor,
                           table_name :str, data: list):
    if table_name == 'genre':
        save_data_to_table_genre(p_curs, table_name, data)
    elif table_name == 'film_work':
        save_data_to_table_film_work(p_curs, table_name, data)
    elif table_name == 'genre_film_work':
        save_data_to_table_genre_film_work(p_curs, table_name, data)
    elif table_name == 'person':
        save_data_to_table_person(p_curs, table_name, data)
    elif table_name == 'person_film_work':
        save_data_to_table_person_film_work(p_curs, table_name, data)


def save_data_to_table_genre(p_curs, table_name,data):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, name, description)
                        VALUES ('{id}', '{name}', '{description}')
                        '''.format(table_name=table_name, id=element.id, name=element.name,
                                   description=element.description)
        p_curs.execute(insert_query)


def save_data_to_table_film_work(p_curs, table_name,data):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, name, description)
                        VALUES ('{id}', '{name}', '{description}')
                        '''.format(table_name=table_name, id=element.id, name=element.name,
                                   description=element.description)
        p_curs.execute(insert_query)

def save_data_to_table_genre_film_work(p_curs, table_name,data):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, name, description)
                        VALUES ('{id}', '{name}', '{description}')
                        '''.format(table_name=table_name, id=element.id, name=element.name,
                                   description=element.description)
        p_curs.execute(insert_query)

def save_data_to_table_person(p_curs, table_name,data):
    for element in data:
        insert_query = '''INSERT INTO content.{table_name} (id, name, description)
                        VALUES ('{id}', '{name}', '{description}')
                        '''.format(table_name=table_name, id=element.id, name=element.name,
                                   description=element.description)
        p_curs.execute(insert_query)

def save_data_to_table_person_film_work(p_curs, table_name,data):
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
