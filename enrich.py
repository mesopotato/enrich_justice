# enrich.py
from db import DBManager  # Assuming DBManager has all necessary DB-related functions
from llama import summarize_text, extract_sachverhalt, extract_entscheid, extract_grundlagen, count_tokens

def process_and_store_summaries(model):
    db = DBManager()
    # Loop until token count is sufficient or you can decide to have a maximum number of retries
    max_retries = 10  # Define maximum retries if necessary to avoid infinite loops
    retries = 0
    min_tokens = 100
    max_tokens = 512        
    
    """Process all entries in e_bern_parsed and store their summaries in e_bern_summary."""

    rows = db.get_all_rows_e_bern_parsed()
    for row in rows:
        parsed_id, pdf_text = row

        # Check if the pdf_text has already been summarized
        if db.is_already_summarized(parsed_id, model):
            print(f"Skipping parsed_id {parsed_id}: already summarized with model {model}")
            continue

        token_count_original = count_tokens(pdf_text)

        # Extract and check summary text
        summary_text = summarize_text(pdf_text, model)
        token_count_summary = count_tokens(summary_text)
        while (token_count_summary < min_tokens or token_count_summary > max_tokens ) and retries < max_retries:
            summary_text = summarize_text(pdf_text, model)
            token_count_summary = count_tokens(summary_text)
            retries += 1
            print(f"Retrying summary for parsed_id {parsed_id} with {token_count_summary} tokens")

        # Extract and check sachverhalt
        sachverhalt = extract_sachverhalt(pdf_text, model)
        token_count_sachverhalt = count_tokens(sachverhalt)
        retries = 0
        while (token_count_sachverhalt < min_tokens or token_count_sachverhalt > max_tokens) and retries < max_retries:
            sachverhalt = extract_sachverhalt(pdf_text, model)
            token_count_sachverhalt = count_tokens(sachverhalt)
            retries += 1
            print(f"Retrying sachverhalt for parsed_id {parsed_id} with {token_count_sachverhalt} tokens")

        # Extract and check entscheid
        entscheid = extract_entscheid(pdf_text, model)
        token_count_entscheid = count_tokens(entscheid)
        retries = 0
        while (token_count_entscheid < min_tokens or token_count_entscheid > max_tokens) and retries < max_retries:
            entscheid = extract_entscheid(pdf_text, model)
            token_count_entscheid = count_tokens(entscheid)
            retries += 1
            print(f"Retrying entscheid for parsed_id {parsed_id} with {token_count_entscheid} tokens")

        # Extract and check grundlagen
        grundlagen = extract_grundlagen(pdf_text, model)
        token_count_grundlagen = count_tokens(grundlagen)
        retries = 0
        while  (token_count_grundlagen < min_tokens or token_count_grundlagen > max_tokens) and retries < max_retries:
            grundlagen = extract_grundlagen(pdf_text, model)
            token_count_grundlagen = count_tokens(grundlagen)
            retries += 1
            print(f"Retrying grundlagen for parsed_id {parsed_id} with {token_count_grundlagen} tokens")

        db.store_summary(parsed_id, summary_text, token_count_original, model, token_count_summary, sachverhalt, token_count_sachverhalt, entscheid, token_count_entscheid, grundlagen, token_count_grundlagen)

def main():
    #db = DBManager()s
    #db.create_summary_table()
    process_and_store_summaries('llama3.1')  # Update this to use the functions from ollama.py if needed

if __name__ == "__main__":
    main()