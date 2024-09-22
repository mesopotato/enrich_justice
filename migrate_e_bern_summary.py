import os
import struct
import psycopg2
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector
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

# Register the vector type
register_vector(postgres_conn)

# Step 1: Create the e_bern_summary table in PostgreSQL
create_table_sql = '''
CREATE TABLE IF NOT EXISTS e_bern_summary (
  id INTEGER PRIMARY KEY,
  tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  parsed_id INTEGER,
  token_count_original INTEGER,
  model TEXT,
  prompt TEXT,
  summary_text TEXT,
  token_count_summary INTEGER,
  sachverhalt TEXT,
  token_count_sachverhalt INTEGER,
  entscheid TEXT,
  token_count_entscheid INTEGER,
  grundlagen TEXT,
  token_count_grundlagen INTEGER,
  summary_vector vector(1536),
  sachverhalt_vector vector(1536),
  entscheid_vector vector(1536),
  grundlagen_vector vector(1536)
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)
mysql_cursor.execute("SELECT * FROM e_bern_summary")
rows = mysql_cursor.fetchall()

# Function to unpack BLOB vectors
def unpack_vector(blob):
    """Convert a binary BLOB back into a list of floats."""
    num_floats = len(blob) // 4  # Each float is 4 bytes
    return list(struct.unpack(f'{num_floats}f', blob))

# Prepare data for insertion
data_list = []
for row in rows:
    # Unpack BLOB vectors
    summary_vector = unpack_vector(row['summary_vector']) if row['summary_vector'] else None
    sachverhalt_vector = unpack_vector(row['sachverhalt_vector']) if row['sachverhalt_vector'] else None
    entscheid_vector = unpack_vector(row['entscheid_vector']) if row['entscheid_vector'] else None
    grundlagen_vector = unpack_vector(row['grundlagen_vector']) if row['grundlagen_vector'] else None

    data = (
        row['ID'], row['tsd'], row['parsed_id'], row['token_count_original'],
        row['model'], row['prompt'], row['summary_text'], row['token_count_summary'],
        row['sachverhalt'], row['token_count_sachverhalt'], row['entscheid'], row['token_count_entscheid'],
        row['grundlagen'], row['token_count_grundlagen'],
        summary_vector, sachverhalt_vector, entscheid_vector, grundlagen_vector
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO e_bern_summary (
    id, tsd, parsed_id, token_count_original, model, prompt, summary_text, token_count_summary,
    sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid,
    grundlagen, token_count_grundlagen,
    summary_vector, sachverhalt_vector, entscheid_vector, grundlagen_vector
) VALUES %s
'''

with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Step 4: Adjust the PostgreSQL sequence for the 'id' column
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT MAX(id) FROM e_bern_summary")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute("SELECT setval(pg_get_serial_sequence('e_bern_summary', 'id'), %s, true)", (max_id,))

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
