import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        # Этот параметр указывает Django, что этот класс не является представлением таблицы
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):

    def __str__(self):
        return self.name

    name = models.CharField('name', max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField('description', blank=True)

    # Если нам нужно добавить данные о самой модели, мы используем класс Meta.
    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"genre"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'


class FilmWork(UUIDMixin, TimeStampedMixin):

    def __str__(self):
        return self.title

    class FilmTypes(models.TextChoices):
        movie = 'movie'
        tv_show = 'tv_show'

    title = models.CharField('name', max_length=255)
    description = models.TextField('description', blank=True)
    creation_date = models.DateField('creation_date')
    rating = models.FloatField('rating', blank=True, default=0,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.CharField(max_length=100, default='movie', choices=FilmTypes.choices)
    #  type = models.TextChoices('movie', 'tv_show')
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"film_work"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = 'Кинопроизведение'
        verbose_name_plural = 'Кинопроизведения'


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"


class Person(UUIDMixin, TimeStampedMixin):

    def __str__(self):
        return self.full_name

    full_name = models.CharField('full_name', max_length=255)

    # Если нам нужно добавить данные о самой модели, мы используем класс Meta.
    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"person"


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField('role', null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
