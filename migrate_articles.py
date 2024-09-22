import psycopg2
from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

# MySQL connection parameters
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_user = os.getenv("MYSQL_USER")
mysql_port = os.getenv("MYSQL_PORT", 3306)
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_db = os.getenv("MYSQL_DATABASE")   

# PostgreSQL connection parameters
postgres_host = os.getenv("POSTGRES_HOST", "localhost")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db =   os.getenv("POSTGRES_DATABASE")

# Connect to MySQL using mysql.connector
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
postgres_conn.autocommit = True  # Automatically commit transactions

# Step 1: Create the articles table in PostgreSQL if it doesn't exist
create_table_sql = '''
CREATE TABLE IF NOT EXISTS articles (
  id INTEGER PRIMARY KEY,
  insert_tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  srn VARCHAR(255),
  shortName VARCHAR(35),
  book_id VARCHAR(35),
  book_name TEXT,
  part_id VARCHAR(35),
  part_name TEXT,
  title_id VARCHAR(35),
  title_name TEXT,
  sub_title_id VARCHAR(35),
  sub_title_name TEXT,
  chapter_id VARCHAR(35),
  chapter_name TEXT,
  sub_chapter_id VARCHAR(35),
  sub_chapter_name TEXT,
  section_id VARCHAR(35),
  section_name TEXT,
  sub_section_id VARCHAR(35),
  sub_section_name TEXT,
  article_id VARCHAR(255),
  article_name TEXT,
  reference TEXT,
  ziffer_id VARCHAR(35),
  ziffer_name TEXT,
  absatz VARCHAR(255),
  text_w_footnotes TEXT
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)  # Use dictionary cursor to access columns by name

mysql_cursor.execute("SELECT * FROM articles")
rows = mysql_cursor.fetchall()

# Step 3: Insert data into PostgreSQL
with postgres_conn.cursor() as postgres_cursor:
    # Prepare the INSERT statement
    insert_sql = '''
    INSERT INTO articles (
        id, insert_tsd, srn, shortName, book_id, book_name,
        part_id, part_name, title_id, title_name, sub_title_id, sub_title_name,
        chapter_id, chapter_name, sub_chapter_id, sub_chapter_name,
        section_id, section_name, sub_section_id, sub_section_name,
        article_id, article_name, reference, ziffer_id, ziffer_name,
        absatz, text_w_footnotes
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    '''

    # Insert each row into PostgreSQL
    for row in rows:
        data = (
            row['id'], row['insert_tsd'], row['srn'], row['shortName'], row['book_id'], row['book_name'],
            row['part_id'], row['part_name'], row['title_id'], row['title_name'], row['sub_title_id'], row['sub_title_name'],
            row['chapter_id'], row['chapter_name'], row['sub_chapter_id'], row['sub_chapter_name'],
            row['section_id'], row['section_name'], row['sub_section_id'], row['sub_section_name'],
            row['article_id'], row['article_name'], row['reference'], row['ziffer_id'], row['ziffer_name'],
            row['absatz'], row['text_w_footnotes']
        )
        postgres_cursor.execute(insert_sql, data)

# Step 4: Adjust the PostgreSQL sequence for the 'id' column
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT MAX(id) FROM articles")
    max_id = cursor.fetchone()[0]
    cursor.execute("SELECT setval(pg_get_serial_sequence('articles', 'id'), %s, true)", (max_id,))

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
