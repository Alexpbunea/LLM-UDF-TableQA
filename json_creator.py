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
                            "unique_id": "001",           
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "Which orders contain negative comments specifically about problems related to shipping or logistics?",
                            "expected_result": [10415, 10417],
                            "udf_justification": "Two LLM UDFs are required. First, a sentiment analysis to detect negative feedback. Second, an entity or topic extraction UDF to verify if the negative comment relates to logistics (shipping problems, incorrect colors, custom instructions, suppliers, etc.). SQL cannot infer context or semantic intent within free text comments."
                        }
                        tables_dataset.append(tables_entry)
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                    """
                    elif table == "orders":
                        ddl = get_create_ddl(conn, table)
                        tables_entry = {
                            "unique_id": "",           # Rellenar luego
                            "db_id": db_folder,
                            "table_name": table,
                            "table_schema": ddl.replace("\n", " ").replace("  ", " ").strip(),       # <-- Aquí va el DDL
                            "question": "",
                            "expected_result": "",
                            "udf_justification": ""
                        }
                        tables_dataset.append(tables_entry)
                    """
                conn.close()

    return tables_dataset

if __name__ == "__main__":
    path = r"C:\Users\Mydevice\Downloads\train\train\train_databases\train_databases"
    dataset = create_tables_dataset(path)
    print(json.dumps(dataset, indent=2, ensure_ascii=False))
