import os
import sqlite3
import json
import pandas as pd

def get_create_ddl(conn, table_name):
    """
    Devuelve la sentencia CREATE TABLE de table_name en la conexión SQLite dada.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type='table' AND name=?;",
        (table_name,)
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

def extract_tables_from_sqlite(sqlite_path):
    """
    Devuelve la lista de nombres de tablas en el fichero SQLite dado.
    """
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

def create_tables_dataset(base_folder):
    tables_dataset = []

    for db_folder in os.listdir(base_folder):
        db_folder_path = os.path.join(base_folder, db_folder)
        if not os.path.isdir(db_folder_path):
            continue

        for sql_file in os.listdir(db_folder_path):


            """
            ORDERS TABLE FROM THE CAR_RETAILS DATABASE
            """
            if sql_file.endswith(".sqlite") and sql_file == "car_retails.sqlite":
                sqlite_path = os.path.join(db_folder_path, sql_file)
                print(f"Procesando: {sqlite_path}")

                # Abrimos una conexión única para extraer DDL
                conn = sqlite3.connect(sqlite_path)

                for table in extract_tables_from_sqlite(sqlite_path):
                    if table == "orders":
                        #pd.set_option('display.max_colwidth', None)
                        #df = pd.read_sql_query("SELECT orderNumber, comments FROM orders WHERE comments IS NOT NULL", conn)
                        #print(df)

                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "shipping_sentiment_entity_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "Which orders contain negative comments specifically about problems related to shipping or logistics?",
                            "expected_result": [10415, 10417],
                            "udf_justification": "Two LLM UDFs are required. First, a sentiment analysis to detect negative feedback. Second, an entity or topic extraction UDF to verify if the negative comment relates to logistics (shipping problems, incorrect colors, custom instructions, suppliers, etc.). SQL cannot infer context or semantic intent within free text comments."
                        }
                        tables_dataset.append(tables_entry)

                """
                RATINGS TABLE FROM THE MOVIE_PLATFORM DATABASE
                """
            elif sql_file.endswith(".sqlite") and sql_file == "movie_platform.sqlite":
                sqlite_path = os.path.join(db_folder_path, sql_file)
                print(f"Procesando: {sqlite_path}")
                
                
                conn = sqlite3.connect(sqlite_path)
                
                for table in extract_tables_from_sqlite(sqlite_path):
                    if table == "ratings":  
                        pd.set_option('display.max_colwidth', None)
                        df = pd.read_sql_query("SELECT rating_id, critic FROM ratings WHERE critic IS NOT NULL LIMIT 60", conn)
                        print(df) 
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "sentiment_sarcasm_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews express negative opinions using a sarcastic or ironic tone?",
                            "expected_result": [9274840],
                            "udf_justification": "Two LLM-based UDFs are required. The first identifies whether the content of the 'critic' field reflects a negative sentiment. The second detects whether the writing style is sarcastic or ironic, which requires pragmatically interpreting the tone of the text. SQL cannot detect sarcasm or judge communicative style. Only a linguistic model with contextual understanding can handle this task."
                        }
                        tables_entry2 = {
                            "unique_id": "sentiment_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews clearly express a negative opinion about the film?",
                            "expected_result": [10312442],
                            "udf_justification": "SQL cannot interpret the emotional tone of text. An LLM-based UDF is required to classify the content of the 'critic' field as negative, since expressions like 'Don’t waste your time on this' reflect strong dissatisfaction."
                        }
                        tables_entry3 = {
                            "unique_id": "writing_style_sarcasm_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews are written in a sarcastic or ironic tone?",
                            "expected_result": [14292624],
                            "udf_justification": "Phrases like 'Can we all just agree to pretend that this flick didn't happen?' clearly reflect sarcasm. SQL cannot infer communicative style. An LLM-based UDF capable of pragmatic and semantic interpretation is required to detect ironic writing."
                        }
                        tables_entry4 = {
                            "unique_id": "critic_summarization_001",
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),
                            "question": "Summarize the content of the review in a single clear sentence.",
                            "expected_result": [
                                {
                                "rating_id": 14558450,
                                "summary": "A review that considers the documentary oversimplified and unfocused."
                                }
                            ],
                            "udf_justification": "This task requires summarizing the 'critic' text into a semantically representative sentence. SQL cannot perform abstract text synthesis, so a summarization-capable LLM UDF is needed."
                        }
                        tables_entry5 = {
                            "unique_id": "sentiment_002",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews express a highly positive opinion and mention personal emotional impact?",
                            "expected_result": [6393245],
                            "udf_justification": "The review states that the movie 'left a mark on me', indicating a strong emotional reaction. SQL cannot detect sentiment intensity or personal impact from unstructured text. A UDF LLM is required to analyze emotional tone and subjective experience."
                        }
                        tables_entry6 = {
                            "unique_id": "sentiment_003",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews express a clearly negative opinion about the film?",
                            "expected_result": [804201],
                            "udf_justification": "The reviewer states they lost empathy and compares the experience to watching 'some sort of Amelie', indicating disillusionment. SQL cannot interpret nuanced emotional expressions. A UDF based on LLM is required to detect negative sentiment in context."
                        }
                        tables_entry7 = {
                            "unique_id": "critic_summarization_002",
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),
                            "question": "Summarize the content of the review in a single clear sentence.",
                            "expected_result": [
                                {
                                "rating_id": 10308984,
                                "summary":  "A heartfelt and artistic documentary that highlights Patti Smith’s reflections on death."
                                }
                            ],
                            "udf_justification": "Summarizing free-text reviews into concise, meaningful sentences requires semantic understanding, which SQL cannot provide. A UDF powered by an LLM is necessary to generate accurate abstractive summaries from open-ended text."
                        }
                        tables_entry8 = {
                            "unique_id": "writing_style_poetic_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews are written in a poetic or abstract style?",
                            "expected_result": [10803157],
                            "udf_justification": "The review uses metaphoric and abstract expressions like 'she extends her hand to the ghosts that walk the streets'. SQL cannot detect poetic language or abstract conceptual framing. A UDF based on LLM is needed to classify the writing style as poetic or literary."
                        }


                        tables_dataset.append(tables_entry)
                        tables_dataset.append(tables_entry2)
                        tables_dataset.append(tables_entry3)
                        tables_dataset.append(tables_entry4)
                        tables_dataset.append(tables_entry5)
                        tables_dataset.append(tables_entry6)
                        tables_dataset.append(tables_entry7)
                        tables_dataset.append(tables_entry8)
                    
                conn.close()

    return tables_dataset

if __name__ == "__main__":
    path = r"C:\Users\Mydevice\Downloads\train\train\train_databases\train_databases"
    dataset = create_tables_dataset(path)
   # print(json.dumps(dataset, indent=2, ensure_ascii=False))
