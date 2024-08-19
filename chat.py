import os
from db import DBManager
from embed import generate_embedding_pure

def combine_and_rank_vectors(similar_summaries_vector_list, similar_sachverhalte_vector_list, 
                             similar_entscheide_vector_list, similar_grundlagen_vector_list, top_n):
    """Combine and rank the top N results from different vector lists."""
    
    # Combine all the vectors into a single list with origin labels
    combined_vectors = []

    for vector in similar_summaries_vector_list:
        combined_vectors.append((vector, "Summary"))

    for vector in similar_sachverhalte_vector_list:
        combined_vectors.append((vector, "Sachverhalt"))

    for vector in similar_entscheide_vector_list:
        combined_vectors.append((vector, "Entscheide"))

    for vector in similar_grundlagen_vector_list:
        combined_vectors.append((vector, "Grundlagen"))

    # Sort the combined vectors by similarity score in descending order
    combined_vectors.sort(key=lambda x: x[0][2], reverse=True)

    # Get the top N vectors
    top_vectors = combined_vectors[:top_n]

    return top_vectors

def find_similar_documents(target_vector, db , top_n):
    """Find similar documents based on user input."""

    summary_vectors = db.get_all_summary_vectors()
    # print lenght of summary_vectors
    print(f'length of summary_vectors: {len(summary_vectors)}')
    #print(f'attributes of summary_vectors: {summary_vectors[0]}')
    sachverhalt_vectors = db.get_all_sachverhalt_vectors()
    entscheid_vectors = db.get_all_entscheid_vectors()
    grundlagen_vectors = db.get_all_grundlagen_vectors()
    
    # Step 2: Compare against all stored vectors in the database
    similar_summaries_vector_list = db.find_similar_vectors(target_vector, summary_vectors, top_n)
    #print(f'length of similar_summaries_vector_list: {len(similar_summaries_vector_list)}')
    #print(f'attributes of similar_summaries_vector_list: {similar_summaries_vector_list[0]}')

    similar_sachverhalte_vector_list = db.find_similar_vectors(target_vector, sachverhalt_vectors, top_n)
    similar_entscheide_vector_list = db.find_similar_vectors(target_vector, entscheid_vectors, top_n)
    similar_grundlagen_vector_list = db.find_similar_vectors(target_vector, grundlagen_vectors, top_n)

    #combined results ranking here 
    # Combine and rank the top 5 results across all categories
    top_combined_vectors = combine_and_rank_vectors(similar_summaries_vector_list, 
                                                    similar_sachverhalte_vector_list, 
                                                    similar_entscheide_vector_list, 
                                                    similar_grundlagen_vector_list, top_n)
    
    # Step 3: Retrieve the actual text based on the similar vectors
    # Retrieve and print the actual text based on the top combined vectors
    print("Top 5 Combined Results:")
    for vector, origin in top_combined_vectors:
        id, parsed_id, similarity = vector
        # Fetch the text corresponding to this vector
        text_info = db.get_texts_from_vectors([(id, parsed_id, vector)])
        if text_info:
            # We expect text_info to be a list, so we get the first element
            text = text_info[0]
            print(f"Origin: {origin}")
            print(f"ID: {text['ID']}, Parsed ID: {text['parsed_id']}, Similarity: {similarity:.4f}")
            if origin == "Summary":
                print(f"Summary: {text['summary_text']}\n")
            if origin == "Sachverhalt":
                print(f"Sachverhalt: {text['sachverhalt']}\n")    
            if origin == "Entscheide":
                print(f"Entscheide: {text['entscheid']}\n")
            if origin == "Grundlagen":
                print(f"Grundlagen: {text['grundlagen']}\n")
            print(f"Forderung: {text['forderung']}\n")
            print(f"File Path: {text['file_path']}\n")

    # Step 3: Retrieve the actual text based on the similar vectors
    #similar_summaries = db.get_texts_from_vectors(similar_summaries_vector_list)
    #similar_sachverhalte = db.get_texts_from_vectors(similar_sachverhalte_vector_list)
    #similar_entscheide = db.get_texts_from_vectors(similar_entscheide_vector_list)
    #similar_grundlagen = db.get_texts_from_vectors(similar_grundlagen_vector_list)

    # Step 4: Display the results similar summaries looks like this : 
    #print("Similar Summaries:")
    #for summary in similar_summaries:
    #    print(summary)
#
    #print("Similar Sachverhalte:")
    #for sachverhalt in similar_sachverhalte:
    #    print(sachverhalt)
#
    #print("Similar Entscheide:")
    #for entscheid in similar_entscheide:
    #    print(entscheid)
#
    #print("Similar Grundlagen:")
    #for grundlagen in similar_grundlagen:
    #    print(grundlagen)

def find_rechtsgrundlage(target_vector, db , top_n):
    
    articles_vectors = db.get_all_articles_vectors()

    # Step 2: Compare against all stored vectors in the database
    similar_vectors = db.find_similar_aritcle_vectors(target_vector, articles_vectors, top_n)

    # Step 3: Retrieve the actual text based on the similar vectors
    similar_articles = db.get_articles_from_vectors(similar_vectors)
    # Step 4: Display the results        
    print("Similar Articles:")
    for article in similar_articles:
        print(article)

def main():
    db = DBManager()
    user_input = input("Please enter your query: ")

    top_n = 5  # Number of similar documents to retrieve

    # Step 1: Generate embedding for the user input
    target_vector = generate_embedding_pure(user_input)

    if target_vector is None:
        print("Failed to generate embedding for user input.")
        return    

    #find_similar_documents(target_vector, db, top_n)

    find_rechtsgrundlage(target_vector, db, top_n)


if __name__ == "__main__":
    main()    
