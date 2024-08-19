from db import DBManager
from embed import generate_embedding

def generate_and_store_abs_embeddings_fedlex(db):
    """Generate embeddings for footnotes and store them in the articles_vectors table."""
    footnotes_data = db.get_all_footnotes_from_articles()
    if not footnotes_data:
        print("No footnotes to process.")
        return

    for entry in footnotes_data:
        # Generate the embedding vector
        vector = generate_embedding(entry['footnote'])

        # Insert the vectors into the database
        db.insert_vector_into_table(
                srn=entry['srn'],
                art_id=entry['art_id'],
                type_cd=entry['type_cd'],
                type_id=entry['type_id'],
                vector=vector,
                source_table=entry['source_table']
            )
def generate_and_store_art_embeddings_fedlex(db):
    """Generate embeddings for footnotes and store them in the articles_vectors table."""
    articles_data = db.get_all_articles_from_articles()
    if not articles_data:
        print("No footnotes to process.")
        return

    for entry in articles_data:
        # Generate the embedding vector
        vector = generate_embedding(entry['full_article'])

        # Insert the vectors into the database
        db.insert_vector_into_table(
                srn=entry['srn'],
                art_id=entry['art_id'],
                type_cd=entry['type_cd'],
                type_id=entry['type_id'],
                vector=vector,
                source_table=entry['source_table']
            )        
def generate_and_store_abs_embeddings_belex(db):
    """Generate embeddings for footnotes and store them in the articles_vectors table."""
    footnotes_data = db.get_all_footnotes_from_articles_bern()
    if not footnotes_data:
        print("No footnotes to process.")
        return

    for entry in footnotes_data:
        # Generate the embedding vector
        vector = generate_embedding(entry['footnote'])

        # Insert the vectors into the database
        db.insert_vector_into_table(
                srn=entry['srn'],
                art_id=entry['art_id'],
                type_cd=entry['type_cd'],
                type_id=entry['type_id'],
                vector=vector,
                source_table=entry['source_table']
            )
def generate_and_store_art_embeddings_belex(db):
    """Generate embeddings for footnotes and store them in the articles_vectors table."""
    articles_data = db.get_all_articles_from_articles_bern()
    if not articles_data:
        print("No footnotes to process.")
        return

    for entry in articles_data:
        # Generate the embedding vector
        vector = generate_embedding(entry['full_article'])

        # Insert the vectors into the database
        db.insert_vector_into_table(
                srn=entry['srn'],
                art_id=entry['art_id'],
                type_cd=entry['type_cd'],
                type_id=entry['type_id'],
                vector=vector,
                source_table=entry['source_table']
            )             

def main():
    db = DBManager()
    #db.drop_table('articles_vector')
    db.create_article_vector_table()
    generate_and_store_abs_embeddings_fedlex(db)
    generate_and_store_art_embeddings_fedlex(db)
    generate_and_store_abs_embeddings_belex(db)
    generate_and_store_art_embeddings_belex(db)

if __name__ == "__main__":
    main()        