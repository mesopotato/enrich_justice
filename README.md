# enrich_justice
does the heavy lifting like db-setup and embedding 
even has a flask Frontend to try it out -> front2.py to tryp over postgres & front.py to see how slowly it is unpacking the BLOBS in MySQL

# getting started 
# create venv : 
python -m venv env
# activate env:
.\env\Scripts\activate
# install dependencies
pip install -r requirements.txt
# to update requirements.txt
pip freeze > requirements.txt

# LLM sherpa Parser setup
https://github.com/nlmatics/nlm-ingestor
## check docker installation
docker --version
## Pull the docker image
docker pull ghcr.io/nlmatics/nlm-ingestor:latest
## Run the docker container
### Start the server and map port 5010 (or your choice) to the container’s internal port 5001
docker run -p 5010:5001 ghcr.io/nlmatics/nlm-ingestor:latest
### verify that the server is running by navigating to http://localhost:5010/ in your web browser


# process_dfs_to_chunks.py
This script loads pdfs from a directory, hierarchically parses the pdfs, chunks and embeds the text and stores the chunk text and the vectors in `e_chunkvectors` table in the db database.

The corresponding frontend is `front3.py` which allows to enter a term which is embedded and then compared to the vectors in `e_chunkvectors`. The top 5 results are returned.

# process_txt_to_chunks.py
This script chunks text from the db table `e_bern_parsed`. It is a simple chunker (chunk size = 512 tokens, overlap = 20). The chunks are embedded and stored as text and vectors in table ` e_chunks_simple`.

The corresponding frontend is `front4.py`.

