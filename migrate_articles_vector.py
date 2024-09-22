import os
import struct
import psycopg2
from psycopg2 import sql
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

# Register the vector type with psycopg2
register_vector(postgres_conn)

# Step 1: Create the articles_vector table in PostgreSQL if it doesn't exist
create_table_sql = '''
CREATE TABLE IF NOT EXISTS articles_vector (
  id INTEGER PRIMARY KEY,
  srn VARCHAR(255),
  art_id VARCHAR(255),
  type_cd VARCHAR(50),
  type_id VARCHAR(255),
  vector vector(1536),
  source_table VARCHAR(255)
);
'''

with postgres_conn.cursor() as cursor:
    cursor.execute(create_table_sql)

# Step 2: Fetch data from MySQL
mysql_cursor = mysql_conn.cursor(dictionary=True)  # Use dictionary cursor to access columns by name
mysql_cursor.execute("SELECT * FROM articles_vector")
rows = mysql_cursor.fetchall()

# Function to unpack BLOB vectors
def unpack_vector(blob):
    """Convert a binary BLOB back into a list of floats."""
    num_floats = len(blob) // 4  # Each float is 4 bytes
    return list(struct.unpack(f'{num_floats}f', blob))

# Prepare data for insertion
data_list = []
for row in rows:
    # Unpack the BLOB vector
    unpacked_vector = unpack_vector(row['vector']) if row['vector'] else None

    # Check vector length
    if unpacked_vector is not None and len(unpacked_vector) != 1536:
        print(f"Warning: Vector length is {len(unpacked_vector)}, expected 1536.")

    data = (
        row['ID'], row['srn'], row['art_id'], row['type_cd'], row['type_id'],
        unpacked_vector, row['source_table']
    )
    data_list.append(data)

# Step 3: Insert data into PostgreSQL using execute_values
insert_sql = '''
INSERT INTO articles_vector (
    id, srn, art_id, type_cd, type_id, vector, source_table
) VALUES %s
'''

with postgres_conn.cursor() as postgres_cursor:
    execute_values(postgres_cursor, insert_sql, data_list)

# Step 4: Adjust the PostgreSQL sequence for the 'id' column
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT MAX(id) FROM articles_vector")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute("SELECT setval(pg_get_serial_sequence('articles_vector', 'id'), %s, true)", (max_id,))

# Close the database connections
mysql_cursor.close()
mysql_conn.close()
postgres_conn.close()
