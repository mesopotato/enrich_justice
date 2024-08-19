from db import DBManager  

from dotenv import load_dotenv
import os
import numpy as np
import re
from openai import OpenAI

import struct

# Load environment variables
load_dotenv()

# Initialize OpenAI client with API key
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_embedding(text, model="text-embedding-3-small"):
    if not isinstance(text, str) or not text.strip():
        print("Invalid or empty text input detected, returning zero vector.")
        return np.zeros(1536)  # Return a zero vector for invalid input
    text = text.replace("\n", " ")  # Normalize newlines
    try:
        print(f"Generating embedding ..")
        response = client.embeddings.create(input=[text], model=model)
        response_dict = response.to_dict()  # Convert the response object to a dictionary
        embedding_vector = response_dict['data'][0]['embedding']
        # Convert the list of floats to a binary format using struct
        binary_embedding = struct.pack(f'{len(embedding_vector)}f', *embedding_vector)
        print(f"Embedding generated successfully.")
        return binary_embedding
    except Exception as e:
        print(f"An error occurred: {e}")
        return np.zeros(1536)  # Return a zero vector if there's an error    
    
def generate_embedding_pure(text, model="text-embedding-3-small"):
    if not isinstance(text, str) or not text.strip():
        print("Invalid or empty text input detected, returning zero vector.")
        return np.zeros(1536)  # Return a zero vector for invalid input
    text = text.replace("\n", " ")  # Normalize newlines
    try:
        print(f"Generating embedding ..")
        response = client.embeddings.create(input=[text], model=model)
        response_dict = response.to_dict()  # Convert the response object to a dictionary
        embedding_vector = response_dict['data'][0]['embedding']
        return embedding_vector    
    except Exception as e:
        print(f"An error occurred: {e}")
        return np.zeros(1536)  # Return a zero vector if there's an error

    

def main():
    db_instance = DBManager()

    # Get all summaries
    summaries = db_instance.get_all_summaries()

    for row in summaries:
        # Unpack the row (assuming the columns are in the order we expect)
        id, parsed_id, summary_text, sachverhalt, entscheid, grundlagen, summary_vector, sachverhalt_vector, entscheid_vector, grundlagen_vector = row

        # Generate and store embeddings for each field

        # Summary Vector
        if summary_text and summary_vector is None:
            summary_vector = generate_embedding(summary_text)
            if summary_vector:
                db_instance.update_summary_vector(id, summary_vector)

        # Sachverhalt Vector
        if sachverhalt and sachverhalt_vector is None:
            sachverhalt_vector = generate_embedding(sachverhalt)
            if sachverhalt_vector:
                db_instance.update_sachverhalt_vector(id, sachverhalt_vector)

        # Entscheid Vector
        if entscheid and entscheid_vector is None:
            entscheid_vector = generate_embedding(entscheid)
            if entscheid_vector:
                db_instance.update_entscheid_vector(id, entscheid_vector)

        # Grundlagen Vector
        if grundlagen and grundlagen_vector is None:
            grundlagen_vector = generate_embedding(grundlagen)
            if grundlagen_vector:
                db_instance.update_grundlagen_vector(id, grundlagen_vector)

if __name__ == "__main__":
    main()