# ollama.py
import ollama
import tiktoken

def summarize_text(text, model,):

    task_instruction = (
    "<INSTRUKTIONEN>\n"
    "Du bist eine deutschsprachige text-analyse KI aus der Schweiz."
    " Die KI extrahiert Informationen aus Entscheidungsdokumenten von Gerichten und Behörden."
    " Deine Aufgabe ist es, folgendes Gerichturteil oder Dokument präzise und wahrheitsgemäss zusammenzufassen."
    " Es dürfen ausschliesslich Fakten wiedergegeben werden, die im Text explizit enthalten sind."
    " Beantworte die Frage am Ende des Textes ausschliesslich auf Deutsch und ohne umschweife."
    " Antworten darfst du nur mit den Informationen, die direkt im Text stehen. "
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."
    "\n<ENDE DER INSTRUKTIONEN>" 

    "\n\n<ANFANG DES ENTSCHEIDUNGSDOKUMENT>\n"
    )
    full_prompt = f"{task_instruction}{text} \n" 
    "<ENDE DES ENTSCHEIDUNGSDOKUMENT> \n\n" 
    
    "<ZUSAMMENFASSUNG>\n"
    "Wie lautet die Zusammenfassung basierend ausschliesslich auf dem vorangehenden Text?"
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."   

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
    
def extract_sachverhalt(text, model):

    task_instruction = (
    "<INSTRUKTIONEN>\n"
    "Du bist eine deutschsprachige text-analyse KI aus der Schweiz."
    " Die KI extrahiert Informationen aus Entscheidungsdokumenten von Gerichten und Behörden."
    " Deine Aufgabe ist es, den Sachverhalt von folgendem Gerichturteil oder Dokument präzise und wahrheitsgemäss zu extrahieren."
    " Extrahiere den Sachverhalt dieses Falles detailliert, ohne dabei Informationen hinzuzufügen oder wegzulassen."
    " Es dürfen ausschliesslich Fakten wiedergegeben werden, die im Text explizit enthalten sind."
    " Beantworte die Frage am Ende des Textes ausschliesslich auf Deutsch und ohne umschweife."
    " Antworten darfst du nur mit den Informationen, die direkt im Text stehen. "
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."
    "\n<ENDE DER INSTRUKTIONEN>" 

    "\n\n<ANFANG DES ENTSCHEIDUNGSDOKUMENT>\n"
    )
    full_prompt = f"{task_instruction}{text} \n" 
    "<ENDE DES ENTSCHEIDUNGSDOKUMENT> \n\n" 
    
    "<EXTRHIERUNG DES SACHVERHALTS>\n"
    "Wie lautet der Sachverhalt basierend ausschliesslich auf dem vorangehenden Text?"
    " Fasse den Sachverhalt dieses Falls detailliert zusammen, ohne dabei Informationen hinzuzufügen oder wegzulassen."
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."        

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

def extract_entscheid(text, model):

    task_instruction = (
    "<INSTRUKTIONEN>\n"
    "Du bist eine deutschsprachige text-analyse KI aus der Schweiz."
    " Die KI extrahiert Informationen aus Entscheidungsdokumenten von Gerichten und Behörden."
    " Deine Aufgabe ist es, die getroffene Entscheidung von folgendem Gerichturteil oder Dokument präzise und wahrheitsgemäss zu extrahieren."
    " Extrahiere die Entscheidung dieses Falles detailliert, ohne dabei Informationen hinzuzufügen oder wegzulassen."
    " Es dürfen ausschliesslich Fakten wiedergegeben werden, die im Text explizit enthalten sind."
    " Beantworte die Frage am Ende des Textes ausschliesslich auf Deutsch und ohne umschweife."
    " Antworten darfst du nur mit den Informationen, die direkt im Text stehen. "
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."
    "\n<ENDE DER INSTRUKTIONEN>" 

    "\n\n<ANFANG DES ENTSCHEIDUNGSDOKUMENT>\n"
    )
    full_prompt = f"{task_instruction}{text} \n" 
    "<ENDE DES ENTSCHEIDUNGSDOKUMENT> \n\n" 
    
    "<EXTRHIERUNG DES ENTSCHEIDS>\n"
    "Wie wurde entschieden basierend ausschliesslich auf dem vorangehenden Text?"
    " Fasse die Entscheidung dieses Falls detailliert zusammen, ohne dabei Informationen hinzuzufügen oder wegzulassen."
    " Wurde der Kläger oder der Beklagte in seinem Anliegen recht gegeben? Warum? "
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."

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

def extract_grundlagen(text, model):
    task_instruction = (
    "<INSTRUKTIONEN>\n"
    "Du bist eine deutschsprachige text-analyse KI aus der Schweiz."
    " Die KI extrahiert Informationen aus Entscheidungsdokumenten von Gerichten und Behörden."
    " Deine Aufgabe ist es, die Rechtsgrundlage von folgendem Gerichturteil präzise und wahrheitsgemäss zu extrahieren."
    " Extrahiere die Rechtsgrundlage dieses Falles detailliert, ohne dabei Informationen hinzuzufügen oder wegzulassen."
    " Es dürfen ausschliesslich Fakten wiedergegeben werden, die im Text explizit enthalten sind."
    " Beantworte die Frage am Ende des Textes ausschliesslich auf Deutsch und ohne umschweife."
    " Antworten darfst du nur mit den Informationen, die direkt im Text stehen. "
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."
    "\n<ENDE DER INSTRUKTIONEN>" 

    "\n\n<ANFANG DES ENTSCHEIDUNGSDOKUMENT>\n"
    )
    full_prompt = f"{task_instruction}{text} \n" 
    "<ENDE DES ENTSCHEIDUNGSDOKUMENT> \n\n" 

    "<EXTRHIERUNG DER RECHTSGRUNDLAGE>\n"
    "Was ist die Rechtsgrundlage dieses Falls basierend ausschliesslich auf dem vorangehenden Text?"
    " Liste die relevanten Gesetze, Verordnungen und Präzedenzfälle auf, die im Text explizit enthalten sind."
    " Führe keine Konversation und stelle keine Fragen. Deine Antwort wird direkt in eine Datenbank gespeichert. Halte dich an die Instruktionen."

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

def count_tokens(text, model="gpt-3.5-turbo"):
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