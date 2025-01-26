import os
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv
from flask import Flask, request, render_template
import struct
import numpy as np

# Use the same OpenAI "client" approach as in your chunk script
from openai import OpenAI

# Load environment variables from .env
load_dotenv()

#############################################
# 1. Configuration: DB + OpenAI / LLM Setup #
#############################################
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DATABASE")

# Create the same OpenAI client used in your chunking script
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

########################################
# 2. Reuse the Same Embedding Function #
########################################
def get_openai_embedding(text, model="text-embedding-3-small"):
    """
    Generate an embedding for the given text using the same function and model
    used to create the database embeddings (text-embedding-3-small).
    """
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


#############################
# 3. Database Connectivity  #
#############################
def connect_to_db():
    """Connect to your PostgreSQL database and return (connection, cursor)."""
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    return conn, conn.cursor()


#######################################
# 4. Vector Similarity Query Function #
#######################################
def find_similar_chunks(target_vector, top_n=5):
    """
    Find the top N chunks in e_chunks_simple by using pgvector-based similarity search.
    """
    conn, cursor = connect_to_db()
    try:
        # Convert the Python list to a string like "[0.1,0.2,0.3,...]"
        vector_str = '[' + ','.join(map(str, target_vector)) + ']'
        
        # Create a SQL literal from that string
        vector_literal = sql.Literal(vector_str)
        
        # Use a consistent placeholder name in the query and in .format(...)
        query = sql.SQL("""
            SELECT
                e_chunks_simple.id,
                e_chunks_simple.e_bern_parsed_id,
                e_chunks_simple.chunk_text,
                e_bern_parsed.file_path,
                1 - ((e_chunks_simple.chunk_vector <=> {vec}::vector(1536))/2) AS similarity
            FROM e_chunks_simple
            join e_bern_parsed on e_chunks_simple.e_bern_parsed_id = e_bern_parsed.id
            WHERE e_chunks_simple.chunk_vector IS NOT NULL
            ORDER BY similarity DESC
            LIMIT %s
        """).format(vec=vector_literal)  # match {vec} in the query to vec=vector_literal

        cursor.execute(query, (top_n,))
        rows = cursor.fetchall()
        return rows

    except psycopg2.Error as e:
        print(f"Error retrieving similar chunks: {e}")
        return []
    finally:
        cursor.close()
        conn.close()




#####################################
# 5. Flask App + Search/Result Page #
#####################################
app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        user_input = request.form.get("query", "")
        top_n = 5  # Adjust how many results you want to display

        # Generate embedding for user input (using the same model)
        query_vector = get_openai_embedding(user_input)
        if not query_vector:
            return render_template("index.html", error="Error generating query embedding.")

        # Query the DB for similar chunks
        similar_rows = find_similar_chunks(query_vector, top_n=top_n)

        # Convert rows to a list of dicts for easier template usage
        results = []
        for row in similar_rows:
            (id, e_bern_parsed_id, chunk_text, file_path, similarity) = row
            
            similarity_val = float(similarity)

            results.append({
                "id": id,
                "e_bern_parsed_id": e_bern_parsed_id,
                "similarity": f"{similarity_val:.4f}",
                "chunk_text": chunk_text,
                "file_path": file_path
            })

        return render_template("results4.html", user_input=user_input, results=results)

    # On GET, just render the search form
    return render_template("index.html")


if __name__ == "__main__":
    # Run the Flask development server
    app.run(debug=True)
