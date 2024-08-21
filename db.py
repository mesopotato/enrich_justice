import os
from mysql.connector import connect, Error
from dotenv import load_dotenv
import struct
from scipy.spatial.distance import cosine
import numpy as np

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
        """Create a table for storing summarized content with vector blobs for various fields."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS e_bern_summary (
                ID INT NOT NULL AUTO_INCREMENT,
                tsd TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                parsed_id INT DEFAULT NULL,
                token_count_original INT DEFAULT NULL,
                model TEXT,
                prompt TEXT,
                summary_text LONGTEXT,
                token_count_summary INT DEFAULT NULL,
                sachverhalt LONGTEXT,
                token_count_sachverhalt INT DEFAULT NULL,
                entscheid LONGTEXT,
                token_count_entscheid INT DEFAULT NULL,
                grundlagen LONGTEXT,
                token_count_grundlagen INT DEFAULT NULL,
                summary_vector BLOB,
                sachverhalt_vector BLOB,
                entscheid_vector BLOB,
                grundlagen_vector BLOB,
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
            cursor.execute("SELECT ID, text_cleaned FROM e_bern_parsed where tokens < 128000 and language = 'de' and text_cleaned is not null")
            rows = cursor.fetchall()
            return rows
        except Error as e:
            print(f"Error retrieving rows from 'e_bern_parsed': {e}")
            return []
        
    def get_document_from_e_bern_parsed_by_id(self, id):
        """Retrieve a specific row by ID from the e_bern_parsed table."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT ID, text_cleaned FROM e_bern_parsed WHERE ID = %s", (id,))
            row = cursor.fetchone()
            return row
        except Error as e:
            print(f"Error retrieving document by ID {id}: {e}")    

    def get_texts_from_vectors(self, vector_list):
        """Retrieve the text content for a list of ID and vector pairs."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            texts = []
            for id, parsed_id, vector in vector_list:
                cursor.execute("SELECT s.ID, s.parsed_id, s.summary_text, s.sachverhalt, s.entscheid, s.grundlagen, r.forderung, e.file_path  FROM e_bern_summary s join e_bern_PARSED e JOIN e_bern_raw r on e.file_name = r.file_name on s.parsed_id = e.ID WHERE s.parsed_id = %s", (parsed_id,))
                row = cursor.fetchone()
                if row:
                    # You can append just the summary_text, or the entire row, depending on what you need
                    texts.append({
                        'ID': row[0],
                        'parsed_id': row[1],
                        'summary_text': row[2],
                        'sachverhalt': row[3],
                        'entscheid': row[4],
                        'grundlagen': row[5],
                        'forderung': row[6],
                        'file_path': row[7]                                             
                    })
            return texts
        except Error as e:
            print(f"Error retrieving texts from vectors: {e}")
            return []           
        
    def get_all_summaries(self):
        """Retrieve all rows for summary, sachverhalt, entscheid, and grundlagen columns."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, parsed_id, summary_text, sachverhalt, entscheid, grundlagen, summary_vector, sachverhalt_vector, entscheid_vector, grundlagen_vector 
                FROM e_bern_summary
                WHERE SUMMARY_VECTOR IS NULL
                OR SACHVERHALT_VECTOR IS NULL
                OR ENTSCHEID_VECTOR IS NULL
                OR GRUNDLAGEN_VECTOR IS NULL                      
            """)
            rows = cursor.fetchall()
            return rows
        except Error as e:
            print(f"Error retrieving all summaries: {e}")
            return []

    def get_summary_by_id(self, id):
        """Retrieve a specific row by ID from the e_bern_summary table."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, parsed_id, summary_text, sachverhalt, entscheid, grundlagen 
                FROM e_bern_summary 
                WHERE ID = %s
            """, (id,))
            row = cursor.fetchone()
            return row
        except Error as e:
            print(f"Error retrieving summary by ID {id}: {e}")
            return None           

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

    def update_summary_vector(self, id, vector_blob):
        """Update the summary_vector for a specific ID."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE e_bern_summary 
                SET summary_vector = %s 
                WHERE ID = %s
            """, (vector_blob, id))
            self.conn.commit()
            print(f"Updated summary_vector for ID {id}.")
        except Error as e:
            print(f"Error updating summary_vector for ID {id}: {e}")

    def update_sachverhalt_vector(self, id, vector_blob):
        """Update the sachverhalt_vector for a specific ID."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE e_bern_summary 
                SET sachverhalt_vector = %s 
                WHERE ID = %s
            """, (vector_blob, id))
            self.conn.commit()
            print(f"Updated sachverhalt_vector for ID {id}.")
        except Error as e:
            print(f"Error updating sachverhalt_vector for ID {id}: {e}")

    def update_entscheid_vector(self, id, vector_blob):
        """Update the entscheid_vector for a specific ID."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE e_bern_summary 
                SET entscheid_vector = %s 
                WHERE ID = %s
            """, (vector_blob, id))
            self.conn.commit()
            print(f"Updated entscheid_vector for ID {id}.")
        except Error as e:
            print(f"Error updating entscheid_vector for ID {id}: {e}")

    def update_grundlagen_vector(self, id, vector_blob):
        """Update the grundlagen_vector for a specific ID."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE e_bern_summary 
                SET grundlagen_vector = %s 
                WHERE ID = %s
            """, (vector_blob, id))
            self.conn.commit()
            print(f"Updated grundlagen_vector for ID {id}.")
        except Error as e:
            print(f"Error updating grundlagen_vector for ID {id}: {e}")

    def unpack_vector(self, blob):
        """Convert a binary BLOB back into a list of floats."""
        num_floats = len(blob) // 4  # Each float is 4 bytes
        return struct.unpack(f'{num_floats}f', blob)        

    def get_all_summary_vectors(self):
    
        """Retrieve all ID and vector pairs from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, parsed_id, summary_vector
                FROM e_bern_summary
                WHERE summary_vector IS NOT NULL
            """)
            rows = cursor.fetchall()
            return [(row[0], row[1], self.unpack_vector(row[2])) for row in rows]
        except Error as e:
            print(f"Error retrieving vectors: {e}")
            return []        

    def get_all_sachverhalt_vectors(self):
        """Retrieve all ID and vector pairs from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, parsed_id, sachverhalt_vector
                FROM e_bern_summary
                WHERE sachverhalt_vector IS NOT NULL
            """)
            rows = cursor.fetchall()
            return [(row[0], row[1], self.unpack_vector(row[2])) for row in rows]
        except Error as e:
            print(f"Error retrieving vectors: {e}")
            return []

    def get_all_entscheid_vectors(self):
        """Retrieve all ID and vector pairs from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, parsed_id, entscheid_vector
                FROM e_bern_summary
                WHERE entscheid_vector IS NOT NULL
            """)
            rows = cursor.fetchall()
            return [(row[0], row[1], self.unpack_vector(row[2])) for row in rows]
        except Error as e:
            print(f"Error retrieving vectors: {e}")
            return []
        
    def get_all_grundlagen_vectors(self):
        """Retrieve all ID and vector pairs from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, parsed_id, grundlagen_vector
                FROM e_bern_summary
                WHERE grundlagen_vector IS NOT NULL
            """)
            rows = cursor.fetchall()
            return [(row[0], row[1], self.unpack_vector(row[2])) for row in rows]
        except Error as e:
            print(f"Error retrieving vectors: {e}")
            return []

    def get_all_articles_vectors(self):
        """Retrieve all ID and vector pairs from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT ID, srn, art_id, type_cd, type_id, vector, source_table
                FROM articles_vector
            """)
            rows = cursor.fetchall()
            # return all attributet of the articles_vector table
            return [(row[0], row[1], row[2], row[3], row[4], self.unpack_vector(row[5]), row[6]) for row in rows]
        except Error as e:
            print(f"Error retrieving vectors: {e}")
            return []
        
    def find_similar_aritcle_vectors(self, target_vector, vectors_list, top_n):
        """Find and return the top N most similar vectors in the database."""
        similarities = []
        for id, srn, art_id, type_cd, type_id, vector, source_table in vectors_list:
            similarity = 1 - cosine(target_vector, vector)  # Cosine similarity
            similarities.append((id, srn, art_id, type_cd, type_id, similarity, vector,  source_table))
        # Sort by similarity in descending order and return the top N results
        similarities.sort(key=lambda x: x[5], reverse=True)
        return similarities[:top_n]

    def get_articles_from_vectors(self, vector_list):
        """Retrieve the text content for a list of ID and vector pairs."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            texts = []
            for id, srn, art_id, type_cd, type_id, similarity, vector, source_table in vector_list:
                if source_table == 'articles':
                    cursor.execute("""SELECT 
                            a.srn, 
                            a.shortName,
                            a.book_name,
                            a.part_name,
                            a.title_name, 
                            a.sub_title_name, 
                            a.chapter_name, 
                            a.sub_chapter_name, 
                            a.section_name, 
                            a.sub_section_name, 
                            a.article_id as art_id,
                            GROUP_CONCAT(
                                CONCAT_WS(' ',
                                    IFNULL(a.article_name, ''),
                                    IFNULL(a.reference, ''),
                                    IFNULL(a.ziffer_name, ''),
                                    IFNULL(a.absatz, ''),
                                    IFNULL(a.text_w_footnotes  , '')
                                ) 
                                ORDER BY a.id SEPARATOR ' '
                            ) AS full_article
                        FROM 
                            articles a
                  
                        WHERE 
                            srn = %s
                            and article_id = %s
 
                        GROUP BY 
                            a.srn, 
                            a.shortName,
                            a.book_name,
                            a.part_name,
                            a.title_name, 
                            a.sub_title_name, 
                            a.chapter_name, 
                            a.sub_chapter_name, 
                            a.section_name, 
                            a.sub_section_name, 
                            a.article_id ;""", (srn, art_id))
                    row = cursor.fetchone()
                    if row:
                    # You can append just the summary_text, or the entire row, depending on what you need
                        texts.append({
                            'srn': row[0],
                            'shortName': row[1],
                            'book_name': row[2],
                            'part_name': row[3],
                            'title_name': row[4],
                            'sub_title_name': row[5],
                            'chapter_name': row[6],
                            'sub_chapter_name': row[7],
                            'section_name': row[8],
                            'sub_section_name': row[9],
                            'art_id': row[10],
                            'full_article': row[11],
                            'source_table': source_table,
                            'similarity': similarity
                            })
                elif source_table == 'articles_bern':
                    cursor.execute("""SELECT                                                  
                            a.systematic_number, 
                            a.abbreviation,
                            a.book_name, 
                            a.part_name, 
                            a.title_name, 
                            a.sub_title_name, 
                            a.chapter_name, 
                            a.sub_chapter_name, 
                            a.section_name, 
                            a.sub_section_name, 
                            a.article_number,
                            GROUP_CONCAT(
                                CONCAT_WS(' ',
                                    IFNULL(a.article_title , ''),
                                    IFNULL(a.paragraph_text , '')
                                ) 
                                ORDER BY a.id SEPARATOR ' '
                            ) AS full_article

                            from articles_bern a 
                            
                            WHERE 
                            a.systematic_number = %s
                            and a.article_number = %s
                                   
                            GROUP BY
                            a.systematic_number,
                            a.abbreviation,
                            a.book_name,
                            a.part_name,
                            a.title_name,
                            a.sub_title_name,
                            a.chapter_name,
                            a.sub_chapter_name,
                            a.section_name,
                            a.sub_section_name,
                            a.article_number
                            ;
                            """, (srn, art_id))
                    row = cursor.fetchone()
                    if row:
                    # You can append just the summary_text, or the entire row, depending on what you need
                        texts.append({
                            'srn': row[0],
                            'shortName': row[1],
                            'book_name': row[2],
                            'part_name': row[3],
                            'title_name': row[4],
                            'sub_title_name': row[5],
                            'chapter_name': row[6],
                            'sub_chapter_name': row[7],
                            'section_name': row[8],
                            'sub_section_name': row[9],
                            'art_id': row[10],
                            'full_article': row[11],
                            'source_table': source_table,
                            'similarity': similarity
                            })
            return texts
        except Error as e:
            print(f"Error retrieving texts from vectors: {e}")
            return []

    def find_similar_vectors(self, target_vector, vectors_list, top_n):

        """Find and return the top N most similar vectors in the database."""
        similarities = []

        for id, parsed_id, vector in vectors_list:
            similarity = 1 - cosine(target_vector, vector)  # Cosine similarity
            similarities.append((id, parsed_id, similarity))

        # Sort by similarity in descending order and return the top N results
        similarities.sort(key=lambda x: x[2], reverse=True)
        return similarities[:top_n]  


    def create_article_vector_table(self):
            """Create a table for storing summarized content with vector blobs for various fields."""
            self.connect()
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS articles_vector (
                        ID INT NOT NULL AUTO_INCREMENT,
                        srn VARCHAR(255) DEFAULT NULL,
                        art_id VARCHAR(255) DEFAULT NULL,
                        type_cd VARCHAR(50) DEFAULT NULL,
                        type_id VARCHAR(255) DEFAULT NULL,
                        vector BLOB,
                        source_table VARCHAR(255) DEFAULT NULL,
                        PRIMARY KEY (ID)
                    )
                """)
                self.conn.commit()
                print("Table articles_vectors created or already exists.")
            except Error as e:
                print(f"Error creating table 'articles_vectors': {e}")     

    def get_all_footnotes_from_articles(self):
        """Fetch footnotes and necessary attributes from the article table for the vector table."""
        self.connect()
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT 
                    a.id,
                    a.srn,
                    a.article_id AS art_id,
                    'abs' AS type_cd,
                    a.id AS type_id,
                    a.text_w_footnotes AS footnote,
                    'articles' as source_table        
                FROM 
                    articles a
                LEFT JOIN 
                    articles_vector av 
                ON 
                    a.srn = av.srn AND
                    a.article_id = av.art_id AND
                    'abs' = av.type_cd AND
                    a.id = av.type_id
                WHERE 
                    a.text_w_footnotes IS NOT NULL
                    AND a.absatz IS NOT NULL
                    and a.absatz != ''       
                    AND av.id IS NULL
                        
                """)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"Error fetching footnotes and attributes: {e}")
            return None    
                       
    def get_all_articles_from_articles(self):
            """Fetch footnotes and necessary attributes from the article table for the vector table."""
            self.connect()
            try:
                cursor = self.conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT 
                            a.srn, 
                            a.article_id as art_id,
                            'art' as type_cd, 
                            a.article_id as type_id, 
                            GROUP_CONCAT(
                                CONCAT_WS(' ',
                                    IFNULL(a.article_name, ''),
                                    IFNULL(a.reference, ''),
                                    IFNULL(a.ziffer_name, ''),
                                    IFNULL(a.absatz, ''),
                                    IFNULL(a.text_w_footnotes  , '')
                                ) 
                                ORDER BY a.id SEPARATOR ' '
                            ) AS full_article, 
                            'articles' as source_table
                        FROM 
                            articles a
                        LEFT JOIN 
                            articles_vector av 
                        ON 
                            a.srn = av.srn AND
                            a.article_id = av.art_id AND
                            'art' = av.type_cd AND
                            a.article_id = av.type_id
                        WHERE 
                            a.text_w_footnotes IS NOT NULL
                            AND av.id IS NULL    
                            
                        GROUP BY 
                            article_id, srn 
                               
                    """)
                result = cursor.fetchall()
                return result
            except Error as e:
                print(f"Error fetching footnotes and attributes: {e}")
                return None   

    def get_all_footnotes_from_articles_bern(self):
            """Fetch footnotes and necessary attributes from the article table for the vector table."""
            self.connect()
            try:
                cursor = self.conn.cursor(dictionary=True)
                cursor.execute("""
                    select 
                        a.id
                        , a.systematic_number as srn 
                        , a.article_number as art_id
                        ,'abs' as type_cd 
                        , a.id as type_id
                        , a.paragraph_text as footnote,
                        'articles_bern' as source_table
                               
                        from articles_bern a 
                        
                        LEFT JOIN 
                            articles_vector av 
                        ON 
                            a.systematic_number = av.srn AND
                            a.article_number = av.art_id AND
                            'abs' = av.type_cd AND
                            a.id = av.type_id
                        WHERE 
                            a.paragraph_text IS NOT NULL
                            AND av.id IS NULL        
                            
                    """)
                result = cursor.fetchall()
                return result
            except Error as e:
                print(f"Error fetching footnotes and attributes: {e}")
                return None   

    def get_all_articles_from_articles_bern(self):
            """Fetch footnotes and necessary attributes from the article table for the vector table."""
            self.connect()
            try:
                cursor = self.conn.cursor(dictionary=True)
                cursor.execute("""
                    select 
                        a.systematic_number as srn, 
                        a.article_number as art_id,
                        'art' as type_cd, 
                        a.article_number as type_id, 
                        GROUP_CONCAT(
                            CONCAT_WS(' ',
                                IFNULL(a.article_title , ''),
                                IFNULL(a.paragraph_text , '')
                            ) 
                            ORDER BY a.id SEPARATOR ' '
                        ) AS full_article,
                        'articles_bern' as source_table	

                    from articles_bern a 
                    
                    LEFT JOIN 
                        articles_vector av 
                    ON 
                        a.systematic_number = av.srn AND
                        a.article_number = av.art_id AND
                        'art' = av.type_cd AND
                        a.article_number = av.type_id
                    WHERE 
                        a.paragraph_text IS NOT NULL
                        AND av.id IS NULL   
                    GROUP BY article_number, systematic_number  
      
                    """)
                result = cursor.fetchall()
                return result
            except Error as e:
                print(f"Error fetching footnotes and attributes: {e}")
                return None                                           

    def insert_vector_into_table(self, srn, art_id, type_cd, type_id, vector, source_table):
        """
        Insert a single vector into the articles_vectors table.
        """
        self.connect()
        try:
            cursor = self.conn.cursor()
            insert_query = """
                INSERT INTO articles_vector (srn, art_id, type_cd, type_id, vector, source_table)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (srn, art_id, type_cd, type_id, vector, source_table))
            self.conn.commit()
            print(f"Inserted vector for srn: {srn}, art_id: {art_id}, type_cd: {type_cd}, type_id: {type_id}, source_table: {source_table}")
        except Error as e:
            print(f"Error inserting vector into articles_vector: {e}")
        finally:
            cursor.close()

# Example usage
if __name__ == "__main__":
    db_manager = DBManager()
    #db_manager.drop_table('e_bern_summary')
    #db_manager.create_summary_table()
    # pull the model with ollama pull to have it available locally
    # llama3 llama3.1 llama3:70B qwen2 mistrl mistral-large mistral-nemo yarn-mistral (bigger context) gemma gemma2 wizardlm2 phi3:14b command-r command-r-plus glm4 aya 
    #db_manager.process_and_store_summaries('llama3.1')
