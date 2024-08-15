from db_manager import MySQL_DBManager
import tiktoken

def count_tokens(text, model="gpt-3.5-turbo"):
    # Load the appropriate tokenizer
    encoding = tiktoken.encoding_for_model(model)
    
    # Encode the text to get the tokens
    tokens = encoding.encode(text)
    
    # Return the number of tokens
    return len(tokens)

def main():
    db_manager = MySQL_DBManager()
    
    try:
        db_manager.connect()

        # Define the table schema
        table_name = "e_bern_parsed_summaries"
        table_schema = """
        ID INT AUTO_INCREMENT PRIMARY KEY,
        id_text INT NOT NULL,
        token_count INT,
        model VARCHAR(255),
        prompt TEXT,
        summary TEXT,
        FOREIGN KEY (id_text) REFERENCES e_bern_parsed(ID)
        """

        # Create the table if it doesn't exist
        db_manager.create_table(table_name, table_schema)

        # Fetch the pdf_text column from the e_bern_parsed table
        query = "SELECT ID, pdf_text FROM e_bern_parsed"
        results = db_manager.fetch_data(query)

        # Process each row and count the tokens
        for row in results:
            record_id = row[0]
            pdf_text = row[1]
            token_count = count_tokens(pdf_text)
            print(f"ID: {record_id}, Token Count: {token_count}")

            # Insert the token count into the e_bern_parsed_summaries table
            insert_query = """
            INSERT INTO e_bern_parsed_summaries (id_text, token_count)
            VALUES (%s, %s)
            """
            db_manager.execute_query(insert_query, (record_id, token_count))



    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    main()

