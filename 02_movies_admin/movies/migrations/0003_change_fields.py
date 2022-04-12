# Generated by Django 3.2 on 2022-04-12 20:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0002_renaming'),
    ]

    operations = [
        migrations.AddField(
            model_name='filmwork',
            name='persons',
            field=models.ManyToManyField(through='movies.PersonFilmWork', to='movies.Person'),
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='role',
            field=models.CharField(choices=[('actor', 'Actor'), ('director', 'Director'), ('writer', 'Writer')], default='', max_length=100, null=True, verbose_name='role'),
        ),
        migrations.AddConstraint(
            model_name='genrefilmwork',
            constraint=models.UniqueConstraint(fields=('film_work_id', 'genre_id'), name='film_work_genre_idx'),
        ),
        migrations.AddConstraint(
            model_name='personfilmwork',
            constraint=models.UniqueConstraint(fields=('film_work_id', 'person_id', 'role'), name='film_work_person_role_idx'),
        ),
    ]
