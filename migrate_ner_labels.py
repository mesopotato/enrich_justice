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

# Step 1: Create the ner_labels table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS ner_labels (
    id INTEGER PRIMARY KEY,
    label VARCHAR(50),
    labelname VARCHAR(255)
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM ner_labels")
rows = mysql_cursor.fetchall()

# Prepare data for insertion
data_list = []
for row in rows:
    data = (
        row['ID'],
        row['label'],
        row['labelname']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO ner_labels (
    id, label, labelname
) VALUES %s
'''

with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Step 4: Adjust the PostgreSQL sequence for the 'id' column (if necessary)
# Since there is no AUTO_INCREMENT in MySQL, we assume 'id' values are manually assigned.
# If you plan to have auto-incrementing IDs in PostgreSQL, you need to create a sequence.

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
