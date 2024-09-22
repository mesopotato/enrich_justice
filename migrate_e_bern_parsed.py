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

# Step 1: Create the e_bern_parsed table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS e_bern_parsed (
  id INTEGER PRIMARY KEY,
  tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  file_name VARCHAR(255) UNIQUE,
  file_path TEXT,
  pdf_text TEXT,
  language VARCHAR(10),
  text_cleaned TEXT,
  tokens INTEGER
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM e_bern_parsed")
rows = mysql_cursor.fetchall()

# Prepare data for insertion
data_list = []
for row in rows:
    data = (
        row['ID'], row['tsd'], row['file_name'], row['file_path'],
        row['pdf_text'], row['language'], row['text_cleaned'], row['tokens']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO e_bern_parsed (
    id, tsd, file_name, file_path, pdf_text,
    language, text_cleaned, tokens
) VALUES %s
'''

with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Step 4: Adjust the PostgreSQL sequence for the 'id' column
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT MAX(id) FROM e_bern_parsed")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute(
            "SELECT setval(pg_get_serial_sequence('e_bern_parsed', 'id'), %s, true)",
            (max_id,)
        )

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
