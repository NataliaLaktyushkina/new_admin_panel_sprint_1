from dataclasses import dataclass, field
import uuid


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
