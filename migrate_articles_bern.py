import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

# MySQL connection parameters
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_user = os.getenv("MYSQL_USER")
mysql_port = int(os.getenv("MYSQL_PORT", 3306))
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_db = os.getenv("MYSQL_DATABASE")

# PostgreSQL connection parameters
postgres_host = os.getenv("POSTGRES_HOST", "localhost")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DATABASE")

# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_password,
    database=mysql_db
)

# Connect to PostgreSQL
postgres_conn = psycopg2.connect(
    host=postgres_host,
    user=postgres_user,
    password=postgres_password,
    dbname=postgres_db
)
postgres_conn.autocommit = True

# Step 1: Create the articles_bern table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS articles_bern (
  id INTEGER PRIMARY KEY,
  insert_tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  systematic_number VARCHAR(255),
  abbreviation VARCHAR(255),
  book_name TEXT,
  part_name TEXT,
  title_name TEXT,
  sub_title_name TEXT,
  chapter_name TEXT,
  sub_chapter_name TEXT,
  section_name TEXT,
  sub_section_name TEXT,
  article_number VARCHAR(255),
  article_title TEXT,
  paragraph_number VARCHAR(255),
  paragraph_text TEXT
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM articles_bern")
rows = mysql_cursor.fetchall()

# Prepare data for insertion
data_list = []
for row in rows:
    data = (
        row['id'], row['INSERT_TSD'], row['systematic_number'], row['abbreviation'],
        row['book_name'], row['part_name'], row['title_name'], row['sub_title_name'],
        row['chapter_name'], row['sub_chapter_name'], row['section_name'], row['sub_section_name'],
        row['article_number'], row['article_title'], row['paragraph_number'], row['paragraph_text']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO articles_bern (
    id, insert_tsd, systematic_number, abbreviation,
    book_name, part_name, title_name, sub_title_name,
    chapter_name, sub_chapter_name, section_name, sub_section_name,
    article_number, article_title, paragraph_number, paragraph_text
) VALUES %s
'''

with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Step 4: Adjust the PostgreSQL sequence for the 'id' column
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT MAX(id) FROM articles_bern")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute("SELECT setval(pg_get_serial_sequence('articles_bern', 'id'), %s, true)", (max_id,))

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
