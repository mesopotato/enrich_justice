import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from psycopg2.extensions import register_adapter
import numpy as np
from dotenv import load_dotenv

class DBManager:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.database = os.getenv("POSTGRES_DATABASE")
        self.conn = None

    def connect(self):
        """Connect to the PostgreSQL database."""
        if not self.conn:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    dbname=self.database
                )
                print(f"Connected to PostgreSQL database at {self.host}")
            except psycopg2.Error as e:
                print(f"Error connecting to PostgreSQL database: {e}")

    def find_similar_vectors(self, target_vector, column_name, top_n):
        """Find and return the top N most similar vectors from the specified column."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            # Convert the vector to a string representation
            vector_str = '[' + ','.join(map(str, target_vector)) + ']'
            # Create a SQL literal for the vector
            vector_literal = sql.Literal(vector_str)
            query = sql.SQL("""
                SELECT id, parsed_id, {column_name}, {column_name} <=> {vector_literal}::vector AS distance
                FROM e_bern_summary
                WHERE {column_name} IS NOT NULL
                ORDER BY distance ASC
                LIMIT %s
            """).format(
                column_name=sql.Identifier(column_name),
                vector_literal=vector_literal
            )
            cursor.execute(query, (top_n,))
            rows = cursor.fetchall()
            # Return list of (id, parsed_id, distance)
            return [(row[0], row[1], row[3]) for row in rows]
        except psycopg2.Error as e:
            print(f"Error retrieving similar vectors: {e}")
            return []

    def find_similar_article_vectors(self, target_vector, top_n):
        """Find and return the top N most similar article vectors."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            vector_str = '[' + ','.join(map(str, target_vector)) + ']'
            vector_literal = sql.Literal(vector_str)
            query = sql.SQL("""
                SELECT id, srn, art_id, type_cd, type_id, vector, source_table, vector <=> {vector_literal}::vector AS distance
                FROM articles_vector
                WHERE vector IS NOT NULL
                ORDER BY distance ASC
                LIMIT %s
            """).format(
                vector_literal=vector_literal
            )
            cursor.execute(query, (top_n,))
            rows = cursor.fetchall()
            return [
                (row[0], row[1], row[2], row[3], row[4], row[7], row[5], row[6])
                for row in rows
            ]
        except psycopg2.Error as e:
            print(f"Error retrieving similar article vectors: {e}")
            return []

    def get_texts_from_vectors(self, vector_list):
        """Retrieve the text content for a list of IDs and similarities."""
        self.connect()
        try:
            cursor = self.conn.cursor(cursor_factory=DictCursor)
            texts = []
            for id, parsed_id, distance in vector_list:
                cursor.execute("""
                    SELECT s.id, s.parsed_id, s.summary_text, s.sachverhalt, s.entscheid, s.grundlagen, r.forderung, e.file_path
                    FROM e_bern_summary s
                    JOIN e_bern_parsed e ON s.parsed_id = e.id
                    JOIN e_bern_raw r ON e.file_name = r.file_name
                    WHERE s.parsed_id = %s
                """, (parsed_id,))
                row = cursor.fetchone()
                if row:
                    texts.append({
                        'id': row['id'],
                        'parsed_id': row['parsed_id'],
                        'summary_text': row['summary_text'],
                        'sachverhalt': row['sachverhalt'],
                        'entscheid': row['entscheid'],
                        'grundlagen': row['grundlagen'],
                        'forderung': row['forderung'],
                        'file_path': row['file_path'],
                        'similarity': distance
                    })
            return texts
        except psycopg2.Error as e:
            print(f"Error retrieving texts from vectors: {e}")
            return []

    def get_articles_from_vectors(self, vector_list):
        """Retrieve the text content for a list of article vectors."""
        self.connect()
        try:
            cursor = self.conn.cursor(cursor_factory=DictCursor)
            texts = []
            for id, srn, art_id, type_cd, type_id, distance, vector, source_table in vector_list:
                if source_table == 'articles':
                    cursor.execute("""
                        SELECT 
                            a.srn, 
                            a.shortname,
                            a.book_name,
                            a.part_name,
                            a.title_name, 
                            a.sub_title_name, 
                            a.chapter_name, 
                            a.sub_chapter_name, 
                            a.section_name, 
                            a.sub_section_name, 
                            a.article_id AS art_id,
                            STRING_AGG(
                                CONCAT_WS(' ',
                                    COALESCE(a.article_name, ''),
                                    COALESCE(a.reference, ''),
                                    COALESCE(a.ziffer_name, ''),
                                    COALESCE(a.absatz, ''),
                                    COALESCE(a.text_w_footnotes, '')
                                ),
                                ' ' ORDER BY a.id
                            ) AS full_article
                        FROM 
                            articles a
                        WHERE 
                            a.srn = %s
                            AND a.article_id = %s
                        GROUP BY 
                            a.srn, 
                            a.shortname,
                            a.book_name,
                            a.part_name,
                            a.title_name, 
                            a.sub_title_name, 
                            a.chapter_name, 
                            a.sub_chapter_name, 
                            a.section_name, 
                            a.sub_section_name, 
                            a.article_id
                    """, (srn, art_id))
                    row = cursor.fetchone()
                    if row:
                        texts.append({
                            'srn': row['srn'],
                            'shortName': row['shortname'],
                            'book_name': row['book_name'],
                            'part_name': row['part_name'],
                            'title_name': row['title_name'],
                            'sub_title_name': row['sub_title_name'],
                            'chapter_name': row['chapter_name'],
                            'sub_chapter_name': row['sub_chapter_name'],
                            'section_name': row['section_name'],
                            'sub_section_name': row['sub_section_name'],
                            'art_id': row['art_id'],
                            'full_article': row['full_article'],
                            'source_table': source_table,
                            'similarity': distance
                        })
                elif source_table == 'articles_bern':
                    cursor.execute("""
                        SELECT                                                  
                            a.systematic_number AS srn, 
                            a.abbreviation,
                            a.book_name, 
                            a.part_name, 
                            a.title_name, 
                            a.sub_title_name, 
                            a.chapter_name, 
                            a.sub_chapter_name, 
                            a.section_name, 
                            a.sub_section_name, 
                            a.article_number AS art_id,
                            STRING_AGG(
                                CONCAT_WS(' ',
                                    COALESCE(a.article_title, ''),
                                    COALESCE(a.paragraph_text, '')
                                ),
                                ' ' ORDER BY a.id
                            ) AS full_article
                        FROM articles_bern a 
                        WHERE 
                            a.systematic_number = %s
                            AND a.article_number = %s
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
                    """, (srn, art_id))
                    row = cursor.fetchone()
                    if row:
                        texts.append({
                            'srn': row['srn'],
                            'shortName': row['abbreviation'],
                            'book_name': row['book_name'],
                            'part_name': row['part_name'],
                            'title_name': row['title_name'],
                            'sub_title_name': row['sub_title_name'],
                            'chapter_name': row['chapter_name'],
                            'sub_chapter_name': row['sub_chapter_name'],
                            'section_name': row['section_name'],
                            'sub_section_name': row['sub_section_name'],
                            'art_id': row['art_id'],
                            'full_article': row['full_article'],
                            'source_table': source_table,
                            'similarity': distance
                        })
            return texts
        except psycopg2.Error as e:
            print(f"Error retrieving articles from vectors: {e}")
            return []
        
