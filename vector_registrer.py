import psycopg2
from pgvector.psycopg2 import register_vector
import os
from dotenv import load_dotenv

load_dotenv()


# PostgreSQL connection parameters
postgres_host = os.getenv("POSTGRES_HOST", "localhost")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db =   os.getenv("POSTGRES_DATABASE")


# Connect to PostgreSQL
postgres_conn = psycopg2.connect(
    host=postgres_host,
    user=postgres_user,
    password=postgres_password,
    dbname=postgres_db
)
postgres_conn.autocommit = True  # 

# Register the vector type with psycopg2
register_vector(postgres_conn)