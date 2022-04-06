import datetime

from dateutil.parser import parse
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import connection
from models import FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork
from connection import *


PAGE_SIZE = 1000


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
            truncate_query = ' '.join(('TRUNCATE content.', table, 'CASCADE; '))
            p_curs.execute(truncate_query)

    except psycopg2.Error as error:
        logging.error('Ошибка сокращения таблицы ', table, error)

    if table == 'genre':
        try:
            query_text = "SELECT name, description, created_at, updated_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                genres = [Genre(*row) for row in curs.fetchmany(PAGE_SIZE)]
                if len(genres):
                    data = modify_genres(genres)
                    save_data_to_table_genre(pg_conn, data)
                    k += len(genres)
                    logging.info(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            logging.error('Ошибка чтения данных, таблица ', table, error)
    elif table == 'person':
        try:
            query_text = "SELECT full_name, created_at, updated_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                persons = [Person(*row) for row in curs.fetchmany(PAGE_SIZE)]
                if len(persons):
                    data = modify_persons(persons)
                    save_data_to_table_person(pg_conn, data)
                    k += len(persons)
                    logging.info(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            logging.error('Ошибка чтения данных, таблица ', table, error)
    elif table == 'film_work':
        try:
            query_text = '''
                SELECT 
                    title, description, creation_date, created_at, updated_at, file_path, type, rating, id
                FROM ''' + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                films = [FilmWork(*row) for row in curs.fetchmany(PAGE_SIZE)]
                if len(films):
                    data = modify_films(films)
                    save_data_to_table_film_work(pg_conn, data)
                    k += len(films)
                    logging.info(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            logging.error('Ошибка чтения данных, таблица ', table, error)
    elif table == 'genre_film_work':
        try:
            query_text = "SELECT film_work_id, genre_id, created_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                genre_films = [GenreFilmWork(*row) for row in curs.fetchmany(PAGE_SIZE)]
                if len(genre_films):
                    data = modify_genre_films(genre_films)
                    save_data_to_table_genre_film_work(pg_conn, data)
                    k += len(genre_films)
                    logging.info(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            logging.error('Ошибка чтения данных, таблица ', table, error)
    elif table == 'person_film_work':
        try:
            query_text = "SELECT film_work_id, person_id, role, created_at, id  FROM " + table + ";"
            curs.execute(query_text)

            k = 0
            while True:
                person_films = [PersonFilmWork(*row) for row in curs.fetchmany(PAGE_SIZE)]
                if len(person_films):
                    data = modify_person_films(person_films)
                    save_data_to_table_person_film_work(pg_conn, data)
                    k += len(person_films)
                    logging.info(' '.join(('Таблица:', table, 'обработано:', str(k), 'строк')))
                else:
                    break

        except sqlite3.Error as error:
            logging.error('Ошибка чтения данных, таблица ', table, error)


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
        logging.error('Ошибка записи данных в таблицу genre', error)


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
        logging.error('Ошибка записи данных в таблицу person', error)


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
        logging.error('Ошибка записи данных в таблицу film_work', error)


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
        logging.error('Ошибка записи данных в таблицу genre_film_work', error)


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
        logging.error('Ошибка записи данных в таблицу person_film_work', error)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    curs = connection.cursor()
    tables_list = create_tables_list()

    for table_name in tables_list:
        get_data_from_table(curs, pg_conn, table_name)


if __name__ == '__main__':
    connection.connect_to_db()
