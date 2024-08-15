import os
from mysql.connector import connect, Error
from dotenv import load_dotenv
import requests
import json
import ollama
import tiktoken

class DBManager:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Retrieve database credentials from environment variables
        self.host = os.getenv("MYSQL_HOST", "localhost")
        self.port = os.getenv("MYSQL_PORT", "3306")  # Default MySQL port
        self.user = os.getenv("MYSQL_USER")
        self.password = os.getenv("MYSQL_PASSWORD")
        self.database = os.getenv("MYSQL_DATABASE")  # Name of the database to connect to
        self.conn = None

    def connect(self):
        """Connect to the MySQL database."""
        if not self.conn:
            try:
                self.conn = connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                print(f"Connected to MySQL database at {self.host}:{self.port}")
            except Error as e:
                print(f"Error connecting to MySQL database: {e}")

    def create_summary_table(self):
        """Create a table for storing summarized content with a foreign key to e_bern_parsed."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS e_bern_summary (
                ID INT AUTO_INCREMENT,
                tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                parsed_id INT,
                token_count_original INT,
                model TEXT, 
                prompt TEXT,
                summary_text LONGTEXT,
                token_count_summary INT,           
                sachverhalt LONGTEXT,
                token_count_sachverhalt INT,           
                entscheid LONGTEXT,
                token_count_entscheid INT,           
                grundlagen LONGTEXT,
                token_count_grundlagen INT,
                PRIMARY KEY (ID)
                )
            """)
            self.conn.commit()
            print("Table e_bern_summary created or already exists.")
        except Error as e:
            print(f"Error creating table 'e_bern_summary': {e}")

    def get_all_rows_e_bern_parsed(self):
        """Retrieve all rows from the e_bern_parsed table."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT ID, pdf_text FROM e_bern_parsed limit 500")
            rows = cursor.fetchall()
            return rows
        except Error as e:
            print(f"Error retrieving rows from 'e_bern_parsed': {e}")
            return []

    def summarize_text(self, text):
        """Summarize the given text using the locally running Llama3.1 model."""
        task_instruction = "Folgender Text ist ein Gerichtsurteil. Fasse den Text detailliert zusammen: \n\n"
        full_prompt = f"{task_instruction}{text}"

        try:
            response = ollama.chat(
                model='llama3.1',
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            summarized_text = response['message']['content']
            return summarized_text
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
            if e.status_code == 404:
                print("Model not found. Make sure the llama3.1 model is running locally.")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to summarize text."
    
    def extract_sachverhalt(self, text):
        """Extract sachverhalt from the given text using Llama3.1 model."""
        task_instruction = "Extrahiere den Sachverhalt aus folgendem Gerichtsurteil:\n\n"
        full_prompt = f"{task_instruction}{text}"

        try:
            response = ollama.chat(
                model='llama3.1',
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            sachverhalt = response['message']['content']
            return sachverhalt
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to extract sachverhalt."

    def extract_entscheid(self, text):
        """Extract entscheid from the given text using Llama3.1 model."""
        task_instruction = "Extrahiere den Entscheid aus folgendem Gerichtsurteil: \n\n"
        full_prompt = f"{task_instruction}{text}"

        try:
            response = ollama.chat(
                model='llama3.1',
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            entscheid = response['message']['content']
            return entscheid
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to extract entscheid."

    def extract_grundlagen(self, text):
        """Extract grundlagen from the given text using Llama3.1 model."""
        task_instruction = "Folgender Text ist ein Gerichtsurteil. Extrahiere die Rechtsgrundlagen:\n\n"
        full_prompt = f"{task_instruction}{text}"

        try:
            response = ollama.chat(
                model='llama3.1',
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            grundlagen = response['message']['content']
            return grundlagen
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to extract grundlagen."    
    
    def count_tokens(self, text, model="gpt-3.5-turbo"):
        """Count the number of tokens in the given text using tiktoken."""
        try:
            # Load the appropriate tokenizer for the given model
            encoding = tiktoken.encoding_for_model(model)
            
            # Encode the text to get the tokens
            tokens = encoding.encode(text)
            
            # Return the number of tokens
            return len(tokens)
        except Exception as e:
            print(f"Error counting tokens: {e}")
            return 0

    def store_summary(self, parsed_id, summary_text, token_count_original, token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen):
        """Store the summarized content and extracted details in the e_bern_summary table."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO e_bern_summary (
                    parsed_id,
                    summary_text,
                    token_count_original,
                    model,       
                    token_count_summary,
                    sachverhalt,
                    token_count_sachverhalt,
                    entscheid,
                    token_count_entscheid,
                    grundlagen,
                    token_count_grundlagen
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (parsed_id, summary_text, token_count_original, 'llama3.1', token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen))
            self.conn.commit()
            print(f"Inserted summary and details for parsed_id {parsed_id}")
        except Error as e:
            print(f"Error inserting summary into 'e_bern_summary': {e}")

    def process_and_store_summaries(self):
        """Process all entries in e_bern_parsed and store their summaries in e_bern_summary."""
        rows = self.get_all_rows_e_bern_parsed()
        for row in rows:
            parsed_id, pdf_text = row
            token_count_original = self.count_tokens(pdf_text)
            summary_text = self.summarize_text(pdf_text)
            token_count_summary = self.count_tokens(summary_text)
            sachverhalt = self.extract_sachverhalt(pdf_text)
            token_count_sachverhalt = self.count_tokens(sachverhalt)
            entscheid = self.extract_entscheid(pdf_text)
            token_count_entscheid = self.count_tokens(entscheid)
            grundlagen = self.extract_grundlagen(pdf_text)
            token_count_grundlagen = self.count_tokens(grundlagen)
            self.store_summary(parsed_id, summary_text, token_count_original, token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen)

    def drop_table(self, table_name):
        """Drops (deletes) a table from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            print(f"Table '{table_name}' dropped successfully.")
        except Error as e:
            print(f"Error dropping table '{table_name}': {e}")            

# Example usage
if __name__ == "__main__":
    db_manager = DBManager()
    db_manager.drop_table('e_bern_summary')
    db_manager.create_summary_table()
    db_manager.process_and_store_summaries()
