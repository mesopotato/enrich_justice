from flask import Flask, request, render_template
from postgresdb import DBManager
from embed import generate_embedding_pure

app = Flask(__name__)

def combine_and_rank_vectors(similar_summaries_vector_list, similar_sachverhalte_vector_list, 
                             similar_entscheide_vector_list, similar_grundlagen_vector_list, top_n):
    """Combine and rank the top N results from different vector lists."""
    
    combined_vectors = []

    for vector in similar_summaries_vector_list:
        combined_vectors.append((vector, "Summary"))

    for vector in similar_sachverhalte_vector_list:
        combined_vectors.append((vector, "Sachverhalt"))

    for vector in similar_entscheide_vector_list:
        combined_vectors.append((vector, "Entscheide"))

    for vector in similar_grundlagen_vector_list:
        combined_vectors.append((vector, "Grundlagen"))

    # Sort by distance in ascending order (lower distance is higher similarity)
    combined_vectors.sort(key=lambda x: x[0][2])  # x[0][2] is the distance

    top_vectors = combined_vectors[:top_n]

    return top_vectors

def find_similar_documents(target_vector, db, top_n):
    """Find similar documents based on user input."""
    
    similar_summaries_vector_list = db.find_similar_vectors(target_vector, 'summary_vector', top_n)
    print('found similar summaries')
    similar_sachverhalte_vector_list = db.find_similar_vectors(target_vector, 'sachverhalt_vector', top_n)
    print('found similar sachverhalte')
    similar_entscheide_vector_list = db.find_similar_vectors(target_vector, 'entscheid_vector', top_n)
    print('found similar entscheide')
    similar_grundlagen_vector_list = db.find_similar_vectors(target_vector, 'grundlagen_vector', top_n)
    print('found similar grundlagen ... combining and ranking vectors')
    
    top_combined_vectors = combine_and_rank_vectors(similar_summaries_vector_list, 
                                                    similar_sachverhalte_vector_list, 
                                                    similar_entscheide_vector_list, 
                                                    similar_grundlagen_vector_list, top_n)
    print('combined and ranked vectors')
    results = []
    for vector, origin in top_combined_vectors:
        id, parsed_id, distance = vector
        text_info = db.get_texts_from_vectors([(id, parsed_id, distance)])
        if text_info:
            text = text_info[0]
            results.append({
                "origin": origin,
                "id": text['id'],
                "parsed_id": text['parsed_id'],
                "similarity": f"{distance:.4f}",
                "text": text['summary_text'],
                "sachverhalt": text['sachverhalt'],
                "entscheid": text['entscheid'],
                "grundlagen": text['grundlagen'],
                "forderung": text['forderung'],
                "file_path": text['file_path']
            })
    return results


def find_rechtsgrundlage(target_vector, db, top_n):
    similar_vectors = db.find_similar_article_vectors(target_vector, top_n)
    print('found similar articles')
    similar_articles = db.get_articles_from_vectors(similar_vectors)
    print('got articles from vectors')
    return similar_articles

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        user_input = request.form["query"]
        top_n = 5  # Number of similar documents to retrieve
        db = DBManager()

        target_vector = generate_embedding_pure(user_input)

        if target_vector is None:
            return render_template("index.html", error="Failed to generate embedding for user input.")
        
        similar_documents = find_similar_documents(target_vector, db, top_n)
        similar_articles = find_rechtsgrundlage(target_vector, db, top_n)
        
        #return render_template("results.html", documents=similar_documents, articles=similar_articles)
        return render_template("results.html",user_input=user_input, documents=similar_documents, articles=similar_articles)

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
