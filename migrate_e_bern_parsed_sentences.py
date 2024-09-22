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

# Step 1: Create the e_bern_parsed_sentences table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS e_bern_parsed_sentences (
  id INTEGER PRIMARY KEY,
  id_text INTEGER NOT NULL,
  sentence TEXT,
  CONSTRAINT fk_id_text_sentences
    FOREIGN KEY(id_text)
    REFERENCES e_bern_parsed(id)
    ON DELETE CASCADE
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

    # Create index on id_text
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_id_text_sentences ON e_bern_parsed_sentences(id_text);')

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM e_bern_parsed_sentences")
rows = mysql_cursor.fetchall()

# Prepare data for insertion
data_list = []
for row in rows:
    data = (
        row['ID'],
        row['id_text'],
        row['sentence']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO e_bern_parsed_sentences (
    id, id_text, sentence
) VALUES %s
'''

with postgres_conn.cursor() as postgres_cursor:
    # Disable foreign key checks temporarily
    postgres_cursor.execute('SET session_replication_role = replica;')
    execute_values(postgres_cursor, insert_sql, data_list)
    # Re-enable foreign key checks
    postgres_cursor.execute('SET session_replication_role = DEFAULT;')

# Step 4: Adjust the PostgreSQL sequence for the 'id' column
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT MAX(id) FROM e_bern_parsed_sentences")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute(
            "SELECT setval(pg_get_serial_sequence('e_bern_parsed_sentences', 'id'), %s, true)",
            (max_id,)
        )

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
