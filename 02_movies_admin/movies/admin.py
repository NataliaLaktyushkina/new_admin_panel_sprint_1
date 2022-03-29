from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmwork, PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmWork


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)
    # Отображение полей в списке
    list_display = ('title', 'type', 'creation_date', 'rating',)


@admin.register(GenreFilmwork)
class GenreFilmworkAdmin(admin.ModelAdmin):
    pass


@admin.register(PersonFilmWork)
class PersonFilmWorkAdmin(admin.ModelAdmin):
    pass
