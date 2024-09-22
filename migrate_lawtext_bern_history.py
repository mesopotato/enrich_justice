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
    database=mysql_db,
    charset='utf8mb4',
    use_unicode=True
)

# Connect to PostgreSQL
postgres_conn = psycopg2.connect(
    host=postgres_host,
    user=postgres_user,
    password=postgres_password,
    dbname=postgres_db
)
postgres_conn.autocommit = True

# Step 1: Create the lawtext_bern_history table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS lawtext_bern_history (
    id INTEGER,
    insert_tsd TIMESTAMP,
    systematic_number VARCHAR(255),
    title VARCHAR(255),
    abbreviation VARCHAR(255),
    enactment VARCHAR(255),
    ingress_author VARCHAR(255),
    ingress_foundation VARCHAR(255),
    ingress_action VARCHAR(255),
    source_url TEXT,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''
with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM lawtext_bern_history")
rows = mysql_cursor.fetchall()

# Prepare data for insertion
data_list = []
for row in rows:
    data = (
        row['ID'],
        row['INSERT_TSD'],
        row['systematic_number'],
        row['title'],
        row['abbreviation'],
        row['enactment'],
        row['ingress_author'],
        row['ingress_foundation'],
        row['ingress_action'],
        row['source_url'],
        row['archived_at']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO lawtext_bern_history (
    id, insert_tsd, systematic_number, title, abbreviation,
    enactment, ingress_author, ingress_foundation, ingress_action,
    source_url, archived_at
) VALUES %s
'''
with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
