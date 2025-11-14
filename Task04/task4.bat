#!/bin/bash
chcp 65001

sqlite3 movies_rating.db < db_init.sql

echo "1. Найти все пары пользователей, оценивших один и тот же фильм. Устранить дубликаты, проверить отсутствие пар с самим собой. Для каждой пары должны быть указаны имена пользователей и название фильма, который они ценили. В списке оставить первые 100 записей."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT DISTINCT
    u1.name AS user1_name,
    u2.name AS user2_name, 
    m.title AS movie_title
FROM ratings r1
JOIN ratings r2 ON r1.movie_id = r2.movie_id AND r1. user_id < r2.user_id
JOIN users u1 ON r1.user_id = u1.id
JOIN users u2 ON r2.user_id = u2.id
JOIN movies m ON r1.movie_id = m.id
LIMIT 100;"
echo " "

echo "2. Найти 10 самых старых оценок от разных пользователей, вывести названия фильмов, имена пользователей, оценку, дату отзыва в формате ГГГГ-ММ-ДД."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT 
movies.title,
users.name,
ratings.rating,
date(ratings.timestamp, 'unixepoch')
FROM movies
JOIN ratings ON movies.id = ratings.movie_id
JOIN users ON ratings.user_id = users.id
ORDER BY ratings.timestamp
LIMIT 10;"
echo " "

echo "3. Вывести в одном списке все фильмы с максимальным средним рейтингом и все фильмы с минимальным средним рейтингом. Общий список отсортировать по году выпуска и названию фильма. В зависимости от рейтинга в колонке 'Рекомендуем' для фильмов должно быть написано 'Да' или 'Нет'."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "WITH movie_ratings AS (
    SELECT 
        movie_id,
        AVG(rating) AS avg_rating
    FROM ratings
    GROUP BY movie_id
),
min_max_ratings AS (
    SELECT 
        MIN(avg_rating) as min_rating,
        MAX(avg_rating) as max_rating
    FROM movie_ratings
)
SELECT 
    m.title,
    m.year,
    mr.avg_rating,
	CASE 
        WHEN mr.avg_rating = (SELECT max_rating FROM min_max_ratings) THEN 'Да'
        WHEN mr.avg_rating = (SELECT min_rating FROM min_max_ratings) THEN 'Нет'
    END AS "Рекомендуем"
FROM movie_ratings mr
JOIN movies m ON mr.movie_id = m.id
WHERE mr.avg_rating = (SELECT min_rating FROM min_max_ratings)
   OR mr.avg_rating = (SELECT max_rating FROM min_max_ratings)
ORDER BY m.year, m.title;"
echo " "

echo "4. Вычислить количество оценок и среднюю оценку, которую дали фильмам пользователи-мужчины в период с 2011 по 2014 год."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT 
	count(*),
	AVG(rating)
FROM
ratings
JOIN users ON ratings.user_id = users.id
WHERE gender = "male" AND ratings.timestamp >= strftime('%s', '2011-01-01') 
AND ratings.timestamp < strftime('%s', '2015-01-01');"
echo " "

echo "5. Составить список фильмов с указанием средней оценки и количества пользователей, которые их оценили. Полученный список отсортировать по году выпуска и названиям фильмов. В списке оставить первые 20 записей."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT 
title,
AVG(rating),
COUNT(user_id),
year
FROM movies
LEFT JOIN ratings ON movies.id = ratings.movie_id
GROUP BY movies.id
ORDER BY year, title
LIMIT 20;"
echo " "

echo "6. Определить самый распространенный жанр фильма и количество фильмов в этом жанре. Отдельную таблицу для жанров не использовать, жанры нужно извлекать из таблицы movies."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "WITH RECURSIVE split(genre, rest) AS (
    SELECT 
        '', 
        genres || '|' 
    FROM movies
    UNION ALL
    SELECT 
        substr(rest, 1, instr(rest, '|') - 1),
        substr(rest, instr(rest, '|') + 1)
    FROM split 
    WHERE rest != ''
),
genre_counts AS (
    SELECT 
        trim(genre) as genre,
        COUNT(*) as count
    FROM split 
    WHERE genre != ''
    GROUP BY genre
)
SELECT 
    genre AS 'Жанр', 
    count AS 'Количество фильмов'  
FROM genre_counts 
ORDER BY count DESC 
LIMIT 1;"
echo " "

echo "7. Вывести список из 10 последних зарегистрированных пользователей в формате 'Фамилия Имя|Дата регистрации' (сначала фамилия, потом имя)."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT 
    substr(name, instr(name, ' ') + 1) || ' ' || 
    substr(name, 1, instr(name, ' ') - 1) || '|' || 
    register_date AS 'Список последних регистраций'
FROM users 
ORDER BY register_date DESC  
LIMIT 10;"
echo " "

echo "8. С помощью рекурсивного CTE определить, на какие дни недели приходился ваш день рождения в каждом году."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "WITH RECURSIVE birthdays(year, date, day_of_week) AS (
    SELECT 
        2005, 
        date('2005-06-27'), 
        strftime('%w', '2005-06-27')
    UNION ALL
    SELECT 
        year + 1, 
        date((year + 1) || '-06-27'), 
        strftime('%w', date((year + 1) || '-06-27'))
    FROM birthdays 
    WHERE year < 2025
)
SELECT 
    year,
    date,
    CASE day_of_week 
        WHEN '0' THEN 'Воскресенье'
        WHEN '1' THEN 'Понедельник' 
        WHEN '2' THEN 'Вторник'
        WHEN '3' THEN 'Среда'
        WHEN '4' THEN 'Четверг'
        WHEN '5' THEN 'Пятница'
        WHEN '6' THEN 'Суббота'
    END AS day_name 
FROM birthdays;"
echo " "
