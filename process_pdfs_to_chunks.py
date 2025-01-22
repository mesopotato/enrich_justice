import os
import psycopg2
from dotenv import load_dotenv
from llmsherpa.readers import LayoutPDFReader
from openai import OpenAI
from psycopg2 import sql, extras
import tiktoken
import requests
import struct
import numpy as np


# Load environment variables
load_dotenv()

# PostgreSQL connection details
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DATABASE")

# PDF directory
PDF_DIR = "Y:\\entscheidsuche"

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def connect_to_db():
    """Connect to the PostgreSQL database and return the connection and cursor."""
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    return conn, conn.cursor()

def create_chunkvectors_table(cursor):
    """Create the e_chunkvectors2 table in the database."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS e_chunkvectors2 (
        chunk_id SERIAL PRIMARY KEY,
        e_bern_raw_id INTEGER REFERENCES e_bern_raw(id),
        doc_name VARCHAR(255),
        doc_path TEXT,
        chunk_index INTEGER,
        chunk_text TEXT,
        chunk_embedding vector(1536),
        section_level INTEGER,
        section_name TEXT,
        token_count INTEGER
    );
    """
    cursor.execute(create_table_query)

# Initialize OpenAI client with API key
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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


def process_pdf(pdf_path, api_url):
    """Parse and process a single PDF file using the self-hosted LayoutPDFReader."""
    try:
        # Send the PDF to the API
        with open(pdf_path, 'rb') as file:
            response = requests.post(api_url, files={'file': file})
            response.raise_for_status()
            document = response.json()

        # Navigate to the expected data structure
        if 'return_dict' not in document:
            raise ValueError(f"Missing 'return_dict' in API response: {document}")

        return_dict = document['return_dict']
        if 'result' not in return_dict:
            raise ValueError(f"Missing 'result' in API response: {document}")

        result = return_dict['result']
        if 'blocks' not in result:
            raise ValueError(f"Missing 'blocks' in API response: {document}")

        # Process the blocks into chunks
        chunks = []
        for block in result['blocks']:
            if 'sentences' in block:
                for sentence in block['sentences']:
                    chunks.append({
                        "text": sentence,  # Sentence text
                        "level": block.get('level', 0),  # Block level
                        "section": block.get('tag', '')  # Tag as section name
                    })

        return chunks
    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed for {pdf_path}: {e}")
    except Exception as e:
        print(f"Failed to process {pdf_path}: {e}")
    return []



def find_pdf_id(cursor, file_name):
    """
    Find the ID of a PDF in the e_bern_raw table by file name.
    Strip the .pdf extension from file_name before querying.
    """
    file_name_without_extension = os.path.splitext(file_name)[0]
    query = "SELECT id FROM e_bern_raw WHERE file_name = %s"
    cursor.execute(query, (file_name_without_extension,))
    result = cursor.fetchone()
    if not result:
        print(f"File not found in database: {file_name_without_extension}")
    return result[0] if result else None

def insert_chunks(cursor, e_bern_raw_id, doc_name, doc_path, chunks):
    """Insert parsed chunks into the e_chunkvectors2 table."""
    insert_query = """
    INSERT INTO e_chunkvectors2 (
        e_bern_raw_id, doc_name, doc_path, chunk_index,
        chunk_text, chunk_embedding, section_level, section_name, token_count
    ) VALUES %s
    """
    data = []
    for i, chunk in enumerate(chunks):
        embedding = get_openai_embedding(chunk["text"])
        token_count = num_tokens_from_string(chunk["text"], "cl100k_base")
        data.append((
            e_bern_raw_id,
            doc_name,
            doc_path,
            i,
            chunk["text"],
            embedding,
            chunk["level"],
            chunk["section"],
            token_count
        ))

    # Batch insert chunks into the database
    extras.execute_values(cursor, insert_query, data)

# Define the API URL
llmsherpa_api_url = "http://localhost:5010/api/parseDocument?renderFormat=all&useNewIndentParser=yes"

def process_pdfs():
    """Main function to process all PDFs and store chunk data in the database."""
    conn, cursor = connect_to_db()
    try:
        create_chunkvectors_table(cursor)
        conn.commit()

        for root, dirs, files in os.walk(PDF_DIR):
            for file in files:
                if file.endswith(".pdf"):
                    pdf_path = os.path.join(root, file)
                    print(f"Processing {pdf_path}")

                    # Process the PDF
                    chunks = process_pdf(pdf_path, llmsherpa_api_url)

                    # Match the PDF file with the e_bern_raw entry
                    e_bern_raw_id = find_pdf_id(cursor, file)
                    if e_bern_raw_id is None:
                        print(f"No matching e_bern_raw ID found for {file}")
                        continue

                    # Insert chunks into the database
                    insert_chunks(cursor, e_bern_raw_id, file, pdf_path, chunks)
                    conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    process_pdfs()
