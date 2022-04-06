import logging
import os
import psycopg2
import sqlite3

from contextlib import contextmanager
from dotenv import load_dotenv
from psycopg2.extras import DictCursor
from load_data import load_from_sqlite


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
        logging.error('Ошибка соединения с sqlite', error)
    finally:
        if conn:
            conn.close()


def get_environment_var():
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    env_var = {'db_name': os.environ.get('DB_NAME'), 'user': os.environ.get('USER_APP'),
               'password': os.environ.get('PASSWORD'), 'host': os.environ.get('HOST'), 'port': os.environ.get('PORT'),
               'db_path': (os.environ.get('DB_PATH'),)}

    return env_var


def connect_to_db():

    env_var = get_environment_var()

    dsl = {'dbname': env_var['db_name'],
           'user': env_var['user'],
           'password': env_var['password'],
           'host': env_var['host'],
           'port': env_var['port']}

    db_path = env_var['db_path']

    logging.basicConfig(filename='loading.log', filemode='w')
    logging.root.setLevel(logging.NOTSET)

    with conn_context(db_path[0]) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
    pg_conn.close()