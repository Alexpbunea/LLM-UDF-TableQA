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
        #     if not sql_file.endswith('.sqlite'):  THIS WAS FOR LOOKING TABLES WITH GROUP BY OR JOIN, AND THERE ARE NO TABLES WITH ANY OF THESE STATMENTS
        #         continue
               
        #     sqlite_path = os.path.join(db_folder_path, sql_file)
        #     #print(f"Procesando: {sqlite_path}")

            
        #     conn = sqlite3.connect(sqlite_path)
        #     for table in extract_tables_from_sqlite(sqlite_path):
        #         ddl = get_create_ddl(conn, table).replace("\n", " ").replace("  ", " ").strip()
        #         if "GROUP BY" in ddl.upper() or "JOIN" in ddl.upper():
        #             print(f"Procesando: {sqlite_path}")
        #         print(ddl, "\n\n")
        #     conn.close()

        
            """
            ORDERS TABLE FROM THE CAR_RETAILS DATABASE
            """
            if sql_file.endswith(".sqlite") and sql_file == "car_retails.sqlite":
                sqlite_path = os.path.join(db_folder_path, sql_file)
                print(f"Processing: {sqlite_path}")

                # Abrimos una conexión única para extraer DDL
                conn = sqlite3.connect(sqlite_path)

                for table in extract_tables_from_sqlite(sqlite_path):
                    if table == "orders":
                        #pd.set_option('display.max_colwidth', None)
                        #df = pd.read_sql_query("SELECT orderNumber, comments FROM orders WHERE comments IS NOT NULL LIMIT 60", conn)
                        #print(df)

                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "shipping_sentiment_entity_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which orders contain negative comments specifically about problems related to shipping or logistics?",
                            "expected_result": [10167, 10179, 10253],#[10415, 10417],
                            "udf_justification": "Two LLM UDFs are required. First, a sentiment analysis to detect negative feedback. Second, an entity or topic extraction UDF to verify if the negative comment relates to logistics (shipping problems, incorrect colors, custom instructions, suppliers, etc.). SQL cannot infer context or semantic intent within free text comments."
                        }
                        
                        tables_entry2 = {
                            "unique_id": "sentiment_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which orders include comments that express a negative sentiment or dissatisfaction from the customer?",
                            "expected_result": [10124, 10164, 10167, 10179, 10253, 10297, 10327, 10328],
                            "udf_justification": "SQL cannot interpret emotional tone or negative intent within unstructured text. A UDF based on LLM is required to classify the sentiment of customer comments, identifying dissatisfaction or concern."
                        }

                        tables_entry3 = {
                            "unique_id": "topic_extraction_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which orders mention specific shipping methods such as FedEx or custom delivery instructions?",
                            "expected_result": [10109, 10127, 10178, 10215, 10233, 10254, 10284, 10308, 10313, 10316, 10319, 10331, 10336],
                            "udf_justification": "The SQL engine cannot extract named shipping methods or understand delivery context from free-text comments. A UDF with named entity recognition is needed to identify and classify references to specific shipping options."
                        }

                        tables_entry4 = {
                            "unique_id": "topic_extraction_002",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which orders include comments related to finance, agreements, or contract renegotiation?",
                            "expected_result": [10148, 10150, 10165, 10186, 10189, 10206, 10223, 10292, 10295],
                            "udf_justification": "Topic classification is required to detect financial negotiation or contractual discussions within free-text comments. SQL lacks the semantic understanding to perform such classification; an LLM-based UDF is necessary."
                        }

                        #tables_entry5 = {
                        #    "unique_id": "sentiment_002",           
                        #    "db_id": db_folder,
                        #    "table_name": table,
                        #     "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                        #    "question": "Which orders include comments that indicate customer dissatisfaction or risk of conflict?",
                        #    "expected_result": [10253, 10327, 10328],
                        #    "udf_justification": "The comments reflect potential issues with the customer (e.g., disputes, dissatisfaction, color mismatches). SQL cannot interpret tone or risk from text. A sentiment and risk evaluation UDF using LLM is required to identify these cases."
                        #}

                        tables_entry6 = {
                            "unique_id": "topic_extraction_003",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which orders involve credit limit issues or payment conditions mentioned by the customer?",
                            "expected_result": [10334, 10212, 10253, 10165],
                            "udf_justification": "This task requires detecting the topic 'credit/payment' within the unstructured comments field. SQL cannot classify the thematic focus of textual content. An LLM-based topic classification UDF is needed."
                        }

                        tables_entry7 = {
                            "unique_id": "topic_extraction_004",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which orders include comments that are about vehicle brands or specific car models?",
                            "expected_result": [10136, 10143, 10231 , 10249],
                            "udf_justification": "This task requires identifying whether the topic of the comment involves vehicle brands or specific car models (e.g., Ferrari, Mustang). SQL cannot detect abstract or thematic intent in text. A topic classification UDF using an LLM is necessary to capture this concept from unstructured comments."
                        }

                        tables_entry8 = {
                            "unique_id": "summarization_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Summarize each order where the customer tried to renegotiate or cancel due to external offers or pricing concerns.",
                            "expected_result": [
                                {
                                "orderID": 10167,
                                "summary": "Customer called to cancel. The warehouse was notified in time and the order didn't ship. They have a new VP of Sales and are shifting their sales model."
                                },
                                {
                                "orderNumber": 10262,
                                "summary": "Customer found a better offer and wants to renegotiate the order."
                                },
                                {
                                "orderNumber": 10239,
                                "summary": "Customer wants to renegotiate the agreement."
                                }
                            ],
                            "udf_justification": "Summarization UDF is needed to condense negotiation-related comments into short, meaningful descriptions. SQL cannot generate summaries from multi-clause, unstructured comments."
                        }    
                        

                        tables_dataset.append(tables_entry)
                        tables_dataset.append(tables_entry2)
                        tables_dataset.append(tables_entry3)
                        tables_dataset.append(tables_entry4)
                        #tables_dataset.append(tables_entry5)
                        tables_dataset.append(tables_entry6)
                        tables_dataset.append(tables_entry7)
                        #tables_dataset.append(tables_entry8)

                """
                RATINGS TABLE FROM THE MOVIE_PLATFORM DATABASE
                """
            elif sql_file.endswith(".sqlite") and sql_file == "movie_platform.sqlite":
                sqlite_path = os.path.join(db_folder_path, sql_file)
                print(f"Processing: {sqlite_path}")
                
                
                conn = sqlite3.connect(sqlite_path)
                
                for table in extract_tables_from_sqlite(sqlite_path):
                    if table == "ratings":  
                        #pd.set_option('display.max_colwidth', None)
                        #df = pd.read_sql_query("SELECT rating_id, critic FROM ratings WHERE critic IS NOT NULL LIMIT 60", conn)
                        #print(df) 
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "sentiment_sarcasm_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews express negative opinions using a sarcastic or ironic tone?",
                            "expected_result": [14292624, 10312442, 9274840, 5859477, 14174187, 14526103, 10312948, 10865786, 10302163, 9023319],
                            "udf_justification": "Two LLM-based UDFs are required. The first identifies whether the content of the 'critic' field reflects a negative sentiment. The second detects whether the writing style is sarcastic or ironic, which requires pragmatically interpreting the tone of the text. SQL cannot detect sarcasm or judge communicative style. Only a linguistic model with contextual understanding can handle this task."
                        }
                        tables_entry2 = {
                            "unique_id": "sentiment_003",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews clearly express a negative opinion about the film?",
                            "expected_result": [10130280, 10312442, 9274840, 5859477, 3459403, 16096612, 15996754, 14558450, 14526103, 14174187, 804201, 10312948, 10302163, 9023319],
                            "udf_justification": "SQL cannot interpret the emotional tone of text. An LLM-based UDF is required to classify the content of the 'critic' field as negative, since expressions like 'Don't waste your time on this' reflect strong dissatisfaction."
                        }
                        tables_entry3 = {
                            "unique_id": "writing_style_sarcasm_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews are written in a sarcastic or ironic tone?",
                            "expected_result": [14292624, 10312442, 9274840, 5859477, 14174187, 14526103, 10312948, 10865786, 10302163, 9023319],
                            "udf_justification": "Phrases like 'Can we all just agree to pretend that this flick didn't happen?' clearly reflect sarcasm. SQL cannot infer communicative style. An LLM-based UDF capable of pragmatic and semantic interpretation is required to detect ironic writing."
                        }
                        tables_entry4 = {
                            "unique_id": "summarization_002",
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),
                            "question": "Summarize the content of the review in a single clear sentence.",
                            "expected_result": [
                                {
                                "rating_id": 10130280,
                                "summary": "The documentary is underwhelming in both content and technical quality."
                                },
                                {
                                "rating_id": 10299920,
                                "summary": "A strong debut film; raises curiosity about the director's future."
                                },
                                {
                                "rating_id": 8761221,
                                "summary": "A harrowing and emotional portrayal of Welsh poverty."
                                },
                                {
                                "rating_id": 14292624,
                                "summary": "The reviewer wishes the film didn't exist."
                                },
                                {
                                "rating_id": 9274840,
                                "summary": "The film is seen as shallow and pretentious."
                                }
                            ],
                            "udf_justification": "This task requires summarizing the 'critic' text into a semantically representative sentence. SQL cannot perform abstract text synthesis, so a summarization-capable LLM UDF is needed."
                        }
                        tables_entry5 = {
                            "unique_id": "sentiment_004",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews express a highly positive opinion and mention personal emotional impact?",
                            "expected_result": [8761221, 14589535, 14247813, 14529999, 6393245],
                            "udf_justification": "The review states that the movie 'left a mark on me', indicating a strong emotional reaction. SQL cannot detect sentiment intensity or personal impact from unstructured text. A UDF LLM is required to analyze emotional tone and subjective experience."
                        }
                        #tables_entry6 = {
                        #    "unique_id": "sentiment_005",           
                        #    "db_id": db_folder,
                        #    "table_name": table,
                        #    "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                        #    "question": "Which reviews express a clearly negative opinion about the film?",
                        #    "expected_result": [804201],
                        #    "udf_justification": "The reviewer states they lost empathy and compares the experience to watching 'some sort of Amelie', indicating disillusionment. SQL cannot interpret nuanced emotional expressions. A UDF based on LLM is required to detect negative sentiment in context."
                       # }
                        #tables_entry7 = {
                        #    "unique_id": "summarization_003",
                        #    "db_id": db_folder,
                        #    "table_name": table,
                        #    "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),
                        #    "question": "Summarize the content of the review in a single clear sentence.",
                        #    "expected_result": [
                        #        {
                        #        "rating_id": 10308984,
                        #        "summary":  "A heartfelt and artistic documentary that highlights Patti Smith's reflections on death."
                        #        }
                        #    ],
                        #    "udf_justification": "Summarizing free-text reviews into concise, meaningful sentences requires semantic understanding, which SQL cannot provide. A UDF powered by an LLM is necessary to generate accurate abstractive summaries from open-ended text."
                        #}
                        tables_entry8 = {
                            "unique_id": "writing_style_poetic_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which reviews are written in a poetic or abstract style?",
                            "expected_result": [14589535, 14247813, 10767324, 10803157, 10308984, 18387699, 18461893, 17922613],
                            "udf_justification": "The review uses metaphoric and abstract expressions like 'she extends her hand to the ghosts that walk the streets'. SQL cannot detect poetic language or abstract conceptual framing. A UDF based on LLM is needed to classify the writing style as poetic or literary."
                        }


                        tables_dataset.append(tables_entry)
                        tables_dataset.append(tables_entry2)
                        tables_dataset.append(tables_entry3)
                        tables_dataset.append(tables_entry4)
                        #tables_dataset.append(tables_entry5)
                        #tables_dataset.append(tables_entry6)
                        #tables_dataset.append(tables_entry7)
                        tables_dataset.append(tables_entry8)
                    
            elif sql_file.endswith(".sqlite") and sql_file == "food_inspection.sqlite":
                sqlite_path = os.path.join(db_folder_path, sql_file)
                print(f"Processing: {sqlite_path}")
                
                
                conn = sqlite3.connect(sqlite_path)
                
                for table in extract_tables_from_sqlite(sqlite_path):
                    if table == "violations":  
                        #pd.set_option('display.max_colwidth', None)
                        #df = pd.read_sql_query("SELECT business_id, description FROM violations WHERE description IS NOT NULL", conn)
                        #print(df[100:150])
                        #print(df[150:200]) 
                        
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "topic_extraction_005",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which violations are related to inadequate handwashing or sanitation facilities?",
                            "expected_result": [10, 24, 31, 45, 56, 58, 108, 121],
                            "udf_justification": "The descriptions must be classified by topic (e.g., hygiene infrastructure). SQL cannot semantically detect that 'inadequate and inaccessible handwashing facilities' refers to hygiene. A topic classification UDF using an LLM is required."
                        }

                        tables_entry2 = {
                            "unique_id": "risk_level_extraction_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which violations represent a high risk to public health?",
                            "expected_result": [10, 58, 73, 77, 80, 88, 95, 121, 126],
                            "udf_justification": "Although some descriptions include the word 'high risk', others may imply it without explicitly saying it. SQL cannot infer risk level from context. A UDF is needed to classify text based on health risk severity."
                        }

                        tables_entry3 = {
                            "unique_id": "entity_extraction_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which violations mention chemicals or toxic substances being improperly used or stored?",
                            "expected_result": [31, 48, 73, 121],
                            "udf_justification": "This task requires identifying references to chemical risks within the violation descriptions. SQL cannot extract such entities or infer meaning from compound phrases. An LLM-based UDF with entity detection capabilities is needed."
                        }

                        tables_entry4 = {
                            "unique_id": "entity_extraction_002",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which businesses have violations that mention the presence of vermin or pests?",
                            "expected_result": [10, 45, 48, 58, 73, 88, 95, 98, 99, 108, 121, 126],
                            "udf_justification": "This requires identifying biological threats such as 'vermin' in the violation descriptions. SQL cannot detect such entities unless exact keywords match. An LLM-based UDF with entity extraction capabilities is necessary."
                        }
                        
                        tables_entry5 = {
                            "unique_id": "summarization_004",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Summarize the main hygiene problems cited in inspections for each business with multiple violations.",
                            "expected_result": [
                                {
                                "business_id": 10,
                                "summary": "Lacks handwashing facilities, unapproved equipment, high risk pests, dirty surfaces, improper food temperatures."
                                },
                                {
                                "business_id": 45,
                                "summary": "Dirty surfaces, unclean utensils, moderate vermin, employee hygiene violations."
                                },
                                {
                                "business_id": 56,
                                "summary": "This business has numerous violations including poor handwashing access, food contamination, and unclean equipment."
                                },
                                {
                                "business_id": 58,
                                "summary": "Multiple vermin levels, unsafe food holding, poor sanitation and cooling practices."
                                },
                                {
                                "business_id": 73,
                                "summary": "Toxic substances, multiple vermin levels, poor food storage, dirty surfaces, plumbing issues."
                                },
                                {
                                "business_id": 88,
                                "summary": "Repeated food contamination, vermin, cooling issues, poor employee hygiene and cleaning."
                                },
                                {
                                "business_id": 95,
                                "summary": "This business was cited for pest issues, improper storage, and unsafe food handling conditions."
                                },
                                {
                                "business_id": 99,
                                "summary": "Poor sanitation, food protection issues, moderate vermin, improper thawing/storage."
                                },
                                {
                                "business_id": 108,
                                "summary": "Unapproved equipment and living quarters, poor sanitation, moderate vermin."
                                },
                                {
                                "business_id": 121,
                                "summary": "All vermin levels present, poor sanitation, toxic substances, improper food storage."
                                },
                                {
                                "business_id": 126,
                                "summary": " Garbage issues, food contamination, repeated vermin, unsafe food temperatures."
                                },
                            ],
                            "udf_justification": "Condensing multiple violations into a coherent summary requires synthesis beyond SQL capabilities. A summarization UDF using an LLM is needed to generate informative descriptions of key hygiene concerns."
                        }

                        tables_entry6 = {
                            "unique_id": "topic_extraction_006",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which businesses have violations indicating unapproved or unmaintained equipment or utensils?",
                            "expected_result": [10, 45, 56, 61, 77, 95, 108],
                            "udf_justification": "Identifying repeated mentions of equipment maintenance issues across multiple descriptions requires semantic topic classification. SQL cannot group variants of this theme; an LLM-based UDF is necessary."
                        }
                        
                        tables_entry7 = {
                            "unique_id": "staff_behaviour_001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which businesses were cited for violations related to employee behavior or actions that compromise food safety?",
                            "expected_result": [45, 116],
                            "udf_justification": "This task requires identifying violations where staff behavior—such as eating, smoking, or lack of certification—directly impacts food safety. SQL cannot infer behavioral categories from text. An LLM-based topic classification UDF is required."
                        }

                        tables_entry8 = {
                            "unique_id": "topic_extraction_007",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       
                            "question": "Which businesses have violations related to garbage service, refuse containers, or waste area sanitation?",
                            "expected_result": [126],
                            "udf_justification": "Detecting violations about unsanitary or missing refuse handling requires understanding the meaning of phrases like 'unsanitary refuse containers'. SQL cannot perform conceptual matching at this level. An LLM-based UDF is required to identify sanitation-related violations regarding waste."
                        }

                        tables_dataset.append(tables_entry)
                        tables_dataset.append(tables_entry2)
                        tables_dataset.append(tables_entry3)
                        tables_dataset.append(tables_entry4)
                        tables_dataset.append(tables_entry5)
                        #tables_dataset.append(tables_entry6)
                        tables_dataset.append(tables_entry7)
                        tables_dataset.append(tables_entry8)

                conn.close()

    return tables_dataset

if __name__ == "__main__":
    path = r"C:\Users\Mydevice\Downloads\train\train\train_databases\train_databases"
    output_path = "./jsons/tables_dataset.json"
    output_path2 = "./jsons/tables_dataset_three_shot.json"
    dataset = create_tables_dataset(path)
    with open(output_path2, "w", encoding="utf-8") as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)
    #print(json.dumps(dataset, indent=2, ensure_ascii=False))
