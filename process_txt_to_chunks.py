import os
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql, extras
import tiktoken
from openai import OpenAI

# Load environment variables
load_dotenv()

# PostgreSQL connection details
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DATABASE")

# Initialize OpenAI client with API key
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Function to calculate the number of tokens in a string
def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

# Connect to the PostgreSQL database
def connect_to_db():
    """Connect to the PostgreSQL database and return the connection and cursor."""
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    return conn, conn.cursor()

# Create the new table for storing chunks
def create_chunks_table(cursor):
    """Create the e_chunks_simple table in the database."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS e_chunks_simple (
        id SERIAL PRIMARY KEY,
        e_bern_parsed_id INTEGER REFERENCES e_bern_parsed(id),
        chunk_nr INTEGER,
        chunk_text TEXT,
        chunk_vector vector(1536),
        tokens INTEGER
    );
    """
    cursor.execute(create_table_query)

# Generate embeddings using OpenAI API
def get_openai_embedding(text, model="text-embedding-3-small"):
    """Generate an embedding for the given text using OpenAI API."""
    if not isinstance(text, str) or not text.strip():
        print("Invalid or empty text input detected, returning zero vector.")
        return [0.0] * 1536  # Return a zero vector for invalid input

    text = text.replace("\n", " ")  # Normalize newlines

    try:
        print("Generating embedding...")
        # Call OpenAI API to generate embeddings
        response = client.embeddings.create(input=[text], model=model)
        response_dict = response.to_dict()  # Convert response to dictionary
        embedding_vector = response_dict['data'][0]['embedding']
        print("Embedding generated successfully.")
        return embedding_vector  # Return raw embedding vector (list of floats)

    except Exception as e:
        print(f"An error occurred while generating embedding: {e}")
        return [0.0] * 1536  # Return a zero vector if there's an error

# Process text into chunks
def chunk_text(text, max_tokens=512, overlap=20):
    """Split the text into chunks of a specified maximum token length with overlap."""
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = encoding.encode(text)
    chunks = []

    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = encoding.decode(chunk_tokens)
        chunks.append(chunk_text)

    return chunks

# Process the e_bern_parsed table and insert chunks into the new table
def process_and_store_chunks():
    """Main function to process text_cleaned from e_bern_parsed and store chunks."""
    conn, cursor = connect_to_db()
    try:
        # Create the new table if it doesn't exist
        create_chunks_table(cursor)
        conn.commit()

        # Retrieve data from e_bern_parsed
        cursor.execute("SELECT id, text_cleaned FROM e_bern_parsed WHERE text_cleaned IS NOT NULL;")
        records = cursor.fetchall()

        for record in records:
            e_bern_parsed_id, text_cleaned = record
            print(f"Processing record ID: {e_bern_parsed_id}")

            # Chunk the text
            chunks = chunk_text(text_cleaned)

            # Prepare data for insertion
            data = []
            for chunk_nr, current_chunk_text in enumerate(chunks):
                if current_chunk_text.strip():  # Ensure chunk_text is not empty
                    chunk_vector = get_openai_embedding(current_chunk_text)
                    tokens = num_tokens_from_string(current_chunk_text, "cl100k_base")
                    data.append((e_bern_parsed_id, chunk_nr, current_chunk_text, chunk_vector, tokens))
                else:
                    print(f"Skipped an empty chunk_text for record ID: {e_bern_parsed_id}")

            # Insert chunks into the database
            insert_query = """
            INSERT INTO e_chunks_simple (
                e_bern_parsed_id, chunk_nr, chunk_text, chunk_vector, tokens
            ) VALUES %s
            """
            extras.execute_values(cursor, insert_query, data)
            conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    process_and_store_chunks()
