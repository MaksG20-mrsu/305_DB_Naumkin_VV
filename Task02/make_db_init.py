import os
import csv
import re
import sqlite3


def escape_sql_string(text):
    if text is None:
        return ""
    return text.replace("'", "''").replace('"', '""')


def generate_sql_script():
    sql_commands = []
    tables = ['movies', 'ratings', 'tags', 'users']
    for table in tables:
        sql_commands.append(f"DROP TABLE IF EXISTS {table};")
    sql_commands.append("""
    CREATE TABLE movies (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        year INTEGER,
        genres TEXT
    );
    """)
    sql_commands.append("""
    CREATE TABLE ratings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        rating REAL NOT NULL,
        timestamp INTEGER NOT NULL
    );
    """)
    sql_commands.append("""
    CREATE TABLE tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        movie_id INTEGER NOT NULL,
        tag TEXT NOT NULL,
        timestamp INTEGER NOT NULL
    );
    """)
    sql_commands.append("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        gender TEXT,
        register_date TEXT,
        occupation TEXT
    );
    """)
    with open('movies.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, 1):
            try:
                movie_id = int(row['movieId'])
                title = escape_sql_string(row['title'])
                genres = escape_sql_string(row['genres'])
                year_match = re.search(r'\((\d{4})\)', row['title'])
                year = year_match.group(1) if year_match else 'NULL'
                title_clean = re.sub(r'\s*\(\d{4}\)\s*$', '', row['title'])
                title_clean = escape_sql_string(title_clean)
                sql_commands.append(
                    f"INSERT INTO movies (id, title, year, genres) "
                    f"VALUES ({movie_id}, '{title_clean}', {year}, '{genres}');"
                )
            except Exception as e:
                continue
    with open('ratings.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, 1):
            try:
                user_id = int(row['userId'])
                movie_id = int(row['movieId'])
                rating = float(row['rating'])
                timestamp = int(row['timestamp'])

                sql_commands.append(
                    f"INSERT INTO ratings (user_id, movie_id, rating, timestamp) "
                    f"VALUES ({user_id}, {movie_id}, {rating}, {timestamp});"
                )
            except Exception as e:
                continue

    with open('tags.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, 1):
            try:
                user_id = int(row['userId'])
                movie_id = int(row['movieId'])
                tag = escape_sql_string(row['tag'])
                timestamp = int(row['timestamp'])

                sql_commands.append(
                    f"INSERT INTO tags (user_id, movie_id, tag, timestamp) "
                    f"VALUES ({user_id}, {movie_id}, '{tag}', {timestamp});"
                )
            except Exception as e:
                continue
    with open('users.txt', 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                parts = line.strip().split('|')
                if len(parts) == 6:
                    user_id = int(parts[0])
                    name = escape_sql_string(parts[1])
                    email = escape_sql_string(parts[2])
                    gender = escape_sql_string(parts[3])
                    register_date = escape_sql_string(parts[4])
                    occupation = escape_sql_string(parts[5])
                    sql_commands.append(
                        f"INSERT INTO users (id, name, email, gender, register_date, occupation) "
                        f"VALUES ({user_id}, '{name}', '{email}', '{gender}', '{register_date}', '{occupation}');"
                    )
            except Exception as e:
                continue
    with open('db_init.sql', 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_commands))


def create_database():
    try:
        conn = sqlite3.connect('movies_rating.db')
        cursor = conn.cursor()
        with open('db_init.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
        commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
        for i, command in enumerate(commands, 1):
            try:
                cursor.execute(command)
            except Exception as e:
                continue
        conn.commit()
        for table in ['movies', 'ratings', 'tags', 'users']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
        conn.close()
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")


if __name__ == '__main__':
    generate_sql_script()
    create_database()
