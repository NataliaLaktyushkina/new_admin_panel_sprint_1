CREATE SCHEMA IF NOT EXISTS content; 

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT not null,
    created timestamp with time zone,
    modified timestamp with time zone
); 

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role TEXT NOT NULL,
    created timestamp with time zone,
    CONSTRAINT FK_filmwork_id FOREIGN KEY content.film_work(id) ON DELETE CASCADE,
    CONSTRAINT FK_person_id FOREIGN KEY content.person(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    Name TEXT NOT NULL, 
    Description TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    created timestamp with time zone,
    CONSTRAINT FK_genre_id FOREIGN KEY content.genre(id) ON DELETE CASCADE,
    CONSTRAINT FK_ilm_work_id FOREIGN KEY content.film_work(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX film_work_person_idx   ON content.person_film_work (film_work_id, person_id);
CREATE UNIQUE INDEX film_work_genre_idx   ON content.genre_film_work (film_work_id, genre_id);