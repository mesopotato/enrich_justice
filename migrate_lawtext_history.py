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

# Step 1: Create the lawtext_history table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS lawtext_history (
    id INTEGER,
    insert_tsd TIMESTAMP,
    srn VARCHAR(255),
    title TEXT,
    preface TEXT,
    preamble TEXT,
    status VARCHAR(255),
    shortname VARCHAR(35),
    beschlussdate VARCHAR(35),
    inkrafttretendate VARCHAR(35),
    quellename VARCHAR(35),
    chronologielink VARCHAR(255),
    changeslink VARCHAR(255),
    sourcelink VARCHAR(255),
    quellelink VARCHAR(255),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''
with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM lawtext_history")
rows = mysql_cursor.fetchall()

# Prepare data for insertion
data_list = []
for row in rows:
    data = (
        row['id'],
        row['insert_tsd'],
        row['srn'],
        row['title'],
        row['preface'],
        row['preamble'],
        row['status'],
        row['shortName'],
        row['beschlussDate'],
        row['inkrafttretenDate'],
        row['quelleName'],
        row['chronologieLink'],
        row['changesLink'],
        row['sourceLink'],
        row['quelleLink'],
        row['archived_at']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO lawtext_history (
    id, insert_tsd, srn, title, preface, preamble, status,
    shortname, beschlussdate, inkrafttretendate, quellename,
    chronologielink, changeslink, sourcelink, quellelink, archived_at
) VALUES %s
'''
with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
