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

    def summarize_text(self, text, model):
        task_instruction = (
        "Du bist ein deutschsprachiger Jurist und hilfst dabei Urteile zusammenzufassen."
        "Bitte ergänze ausschließlich auf Deutsch. "
        "Folgender Text ist ein Gerichtsurteil. "
        "Fasse diesem Fall detailliert zusammen: \n\n"
        )
        full_prompt = f"{task_instruction}{text} \n\n Zusammenfassung  in Bezug auf den vorangehenden Text:\n <hier einfügen> "

        try:
            response = ollama.chat(
                model=model,
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            summarized_text = response['message']['content']
            return summarized_text
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
            if e.status_code == 404:
                print("Model not found. Make sure the model is running locally.")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to summarize text."
    
    def extract_sachverhalt(self, text, model):
        task_instruction = (
        "Du bist ein deutschsprachiger Jurist und hilfst dabei Urteile zusammenzufassen."
        "Bitte ergänze ausschließlich auf Deutsch. "
        "Folgender Text ist ein Gerichtsurteil. "
        "Wie lautet der genaue Sachverhalt in diesem Fall? \n\n"
        )
        full_prompt = f"{task_instruction}{text} \n\n Sachverhalt in Bezug auf den vorangehenden Text:\n <hier einfügen> "

        try:
            response = ollama.chat(
                model=model,
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            sachverhalt = response['message']['content']
            return sachverhalt
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to extract sachverhalt."

    def extract_entscheid(self, text, model):
        task_instruction = (
        "Du bist ein deutschsprachiger Jurist und hilfst dabei Urteile zusammenzufassen."
        "Bitte ergänze ausschließlich auf Deutsch. "
        "Folgender Text ist ein Gerichtsurteil. "
        "Wie wurde in diesem Fall entschieden?: \n\n"
        
        )
        full_prompt = f"{task_instruction}{text} \n\n Entscheid in Bezug auf den vorangehenden Text:\n <hier einfügen> "

        try:
            response = ollama.chat(
                model=model,
                messages=[{'role': 'user', 'content': full_prompt}]
            )
            entscheid = response['message']['content']
            return entscheid
        except ollama.ResponseError as e:
            print(f"Error: {e.error}")
        except Exception as err:
            print(f"An error occurred: {err}")
        
        return "Failed to extract entscheid."

    def extract_grundlagen(self, text, model):
        task_instruction = (
        "Du bist ein deutschsprachiger Jurist und hilfst dabei Urteile zusammenzufassen. "
        "Bitte ergänze ausschließlich auf Deutsch. "
        "Folgender Text ist ein Gerichtsurteil. "
        "Extrahiere die Rechtsgrundlage und die für den Entscheid relevanten Artikel präzise und klar. Welche Aritkel sind zur Anwedung gekommen?\n\n"
    )
        full_prompt = f"{task_instruction}{text} \n\n Rechtsgrundlage in Bezug auf den vorangehenden Text:\n <hier einfügen> "

        try:
            response = ollama.chat(
                model=model,
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

    def store_summary(self, parsed_id, summary_text, token_count_original, model, token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen):
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
                ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (parsed_id, summary_text, token_count_original, model, token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen))
            self.conn.commit()
            print(f"Inserted summary and details for parsed_id {parsed_id}")
        except Error as e:
            print(f"Error inserting summary into 'e_bern_summary': {e}")

    def process_and_store_summaries(self, model):
        """Process all entries in e_bern_parsed and store their summaries in e_bern_summary."""
        rows = self.get_all_rows_e_bern_parsed()
        for row in rows:
            parsed_id, pdf_text = row

            # Check if the pdf_text has already been summarized
            if self.is_already_summarized(parsed_id, model):
                print(f"Skipping parsed_id {parsed_id}: already summarized with model {model}")
                continue

            token_count_original = self.count_tokens(pdf_text)
            summary_text = self.summarize_text(pdf_text, model)
            token_count_summary = self.count_tokens(summary_text)
            sachverhalt = self.extract_sachverhalt(pdf_text, model)
            token_count_sachverhalt = self.count_tokens(sachverhalt)
            entscheid = self.extract_entscheid(pdf_text, model)
            token_count_entscheid = self.count_tokens(entscheid)
            grundlagen = self.extract_grundlagen(pdf_text, model)
            token_count_grundlagen = self.count_tokens(grundlagen)
            self.store_summary(parsed_id, summary_text, token_count_original, model, token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen)

    def is_already_summarized(self, parsed_id, model):
        """Check if the given parsed_id has already been summarized in e_bern_summary."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM e_bern_summary WHERE parsed_id = %s and model = %s LIMIT 1", (parsed_id, model))
            result = cursor.fetchone()
            return result is not None
        except Error as e:
            print(f"Error checking if parsed_id {parsed_id} is already summarized: {e}")
            return False

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
    #db_manager.drop_table('e_bern_summary')
    #db_manager.create_summary_table()

    # pull the model with ollama pull to have it available locally
    # llama3 llama3.1 llama3:70B qwen2 mistrl mistral-large mistral-nemo yarn-mistral (bigger context) gemma gemma2 wizardlm2 phi3:14b command-r command-r-plus glm4
    db_manager.process_and_store_summaries('gemma2')
