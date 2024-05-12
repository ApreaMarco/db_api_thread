import sqlite3
from os.path import join, dirname


class DatabaseHandler:
    def __init__(self):
        self._filename_db = join(dirname(__file__), "../data", "library.db")
        self._connection = None

    def get_db_connection(self):
        if self._connection is None:
            print("DatabaseHandler: Nuova Connessione!")
            self._connection = self.get_connection()
        else:
            print("DatabaseHandler: Riutilizzo Connessione!")
        return self._connection

    def get_connection(self):
        conn = sqlite3.connect(self._filename_db)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        print(f"DatabaseHandler: Il database {self._filename_db} è stato aperto correttamente!")
        return conn

    def commit_connection(self):
        print(f"DatabaseHandler: Commit completata con successo!")
        self._connection.commit()

    def close_connection(self):
        self._connection.close()
        print(f"DatabaseHandler: Il database {self._filename_db} è stato chiuso correttamente")


def validate_database_handler(database_handler=None):
    if database_handler is None and not isinstance(database_handler, DatabaseHandler):
        print("DatabaseHandler: Creazione di una nuova istanza!")
        database_handler = DatabaseHandler()
    else:
        print("DatabaseHandler: Riutilizzo dell'istanza!")
    return database_handler


def table_scuole_model():
    return {"columns": [("COSCUO", "TEXT"),
                        ("DESIS", "TEXT"),
                        ("TIPO_SCUOLA", "TEXT"),
                        ("NOMSCU", "TEXT"),
                        ("INDSCU", "TEXT"),
                        ("CAPSCU", "TEXT"),
                        ("LOCSCU", "TEXT")],
            "pk": ["COSCUO"],
            "fk": [],
            "check": []}


def table_classi_model():
    return {"columns": [("CLID", "INTEGER"),
                        ("COSCUO", "TEXT"),
                        ("ASCO", "TEXT"),
                        ("CLASSE", "TEXT"),
                        ("SEZION", "TEXT"),
                        ("DESCOMB", "TEXT")],
            "pk": ["CLID"],
            "fk": [("COSCUO", "scuole", "COSCUO")],
            "check": []}


def table_libri_model():
    return {"columns": [("EAN", "TEXT"),
                        ("Publisher", "TEXT"),
                        ("Author", "TEXT"),
                        ("Title", "TEXT"),
                        ("PublicationDate", "TEXT"),
                        ("ReleaseDate", "TEXT"),
                        ("DetailPageURL", "TEXT"),
                        ("LargeImage", "TEXT"),
                        ("Price", "NUMERIC(5,2)")],
            "pk": ["EAN"],
            "fk": [],
            "check": []}


def table_adozioni_model():
    return {"columns": [("ISBN", "TEXT"),
                        ("CLID", "INTEGER"),
                        ("COSCUO", "TEXT"),
                        ("DESC_DISCIPLINA", "TEXT"),
                        ("NUOVA_ADOZIONE", "TEXT"),
                        ("CONSIGLIATO", "TEXT"),
                        ("DA_ACQUISTARE", "TEXT")],
            "pk": ["ISBN", "CLID", "COSCUO"],
            "fk": [("ISBN", "libri", "EAN"),
                   ("CLID", "classi", "CLID"),
                   ("COSCUO", "scuole", "COSCUO")],
            "check": [("NUOVA_ADOZIONE", ('Si', 'No')),
                      ("CONSIGLIATO", ('Si', 'No')),
                      ("DA_ACQUISTARE", ('Si', 'No'))]}


def tables_model():
    return {"scuole": table_scuole_model(),
            "classi": table_classi_model(),
            "libri": table_libri_model(),
            "adozioni": table_adozioni_model()}


def format_columns(columns):
    return ", ".join([f"{col[0]} {col[1]}" for col in columns])


def format_primary_key(pk):
    return f", PRIMARY KEY ({', '.join(pk)})" if pk else ""


def format_foreign_keys(fk):
    return ", ".join([f"FOREIGN KEY ({col[0]}) REFERENCES {col[1]}({col[2]})" for col in fk])


def format_checks(check):
    return ", ".join([f"CHECK ({col[0]} IN {col[1]})" for col in check])


def create_table_sql(table_name, table_model):
    columns_sql = format_columns(table_model["columns"])
    pk_sql = format_primary_key(table_model["pk"])
    fk_sql = format_foreign_keys(table_model["fk"])
    check_sql = format_checks(table_model["check"])

    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql}{pk_sql}{fk_sql}{check_sql})"
    return sql


def select_row(table_name, pk_name, pk_value, database_handler=None):
    database_handler = validate_database_handler(database_handler)
    connection = database_handler.get_db_connection()
    query = f"SELECT * FROM {table_name} WHERE {pk_name} = ?"
    cursor = connection.execute(query, (pk_value,))
    return cursor.fetchone()


def update_row(table_name, columns, values, pk_name, pk_value, database_handler=None):
    database_handler = validate_database_handler(database_handler)
    connection = database_handler.get_db_connection()
    set_clause = ", ".join([f"{column} = ?" for column in columns])
    query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_name} = ?"
    parameters = (*values, pk_value)  # Creazione dei parametri per la query
    connection.execute(query, parameters)  # Esecuzione della query con i parametri


def insert_row(table_name, columns, values, database_handler=None):
    database_handler = validate_database_handler(database_handler)
    connection = database_handler.get_db_connection()
    columns_string = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in values])  # Generazione dei segnaposto
    query = f"INSERT INTO {table_name} ({columns_string}) VALUES ({placeholders})"
    connection.execute(query, values)


def create_tables(tables_model_structure, database_handler=None):
    database_handler = validate_database_handler(database_handler)
    connection = database_handler.get_db_connection()
    for table_name in tables_model_structure.keys():
        sql = create_table_sql(table_name, tables_model_structure[table_name])
        connection.execute(sql)
        connection.commit()
        print(f"DatabaseHandler: La tabella '{table_name}' è stata creata con successo!")


def retrieve_classe_ids(scuola_id, database_handler=None):
    database_handler = validate_database_handler(database_handler)
    connection = database_handler.get_db_connection()
    query = "SELECT CLID FROM classi WHERE COSCUO = ?"
    classe_ids = [row[0] for row in connection.execute(query, (scuola_id,)).fetchall()]
    return classe_ids
