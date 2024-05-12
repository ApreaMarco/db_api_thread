from threading import get_ident, RLock, Semaphore
from model.library import (DatabaseHandler, table_scuole_model, table_classi_model, table_libri_model,
                           table_adozioni_model, tables_model, select_row, update_row, insert_row, retrieve_classe_ids)
from api.adozioni_amazon_api import (APIHandler, get_scuola, get_classi_scuola, get_libro, get_libri_adottati,
                                     fetch_data_from_source, retry_session)


class DatabaseAPIHandler:
    def __init__(self):
        self._ident = get_ident()
        self._lock = RLock()
        self._semaphore = Semaphore()
        self._db_handler = DatabaseHandler()
        self._api_handler = APIHandler()

    def acquire_semaphore(self):
        self._semaphore.acquire()

    def release_semaphore(self):
        self._semaphore.release()

    def get_db_handler(self):
        with self._lock:
            return self._db_handler

    def get_api_handler(self):
        with self._lock:
            return self._api_handler

    def commit_connection(self):
        with self._lock:
            self._db_handler.commit_connection()

    def close_connection(self):
        with self._lock:
            self._db_handler.close_connection()

    @property
    def ident(self):
        return self._ident


def validate_database_api_handler(database_api_handler=None):
    if database_api_handler is None or not isinstance(database_api_handler, DatabaseAPIHandler):
        database_api_handler = DatabaseAPIHandler()
        print(f"Creo una nuova istanza di DatabaseAPIHandler con identificatore {database_api_handler.ident}!")
    else:
        print(f"Riutilizzo l'istanza di DatabaseAPIHandler con identificatore {database_api_handler.ident}!")
    return database_api_handler


def scuola_control():
    return {"source": get_scuola,
            "source_params": {"scuola_id": "VRTF03000V",
                              "comune_id": "Verona",
                              "grado_id": 2,
                              "api_handler": None},
            "prepare_dict": scuola_dict_structure,
            "structure_check": check_scuola_dict_structure}


def classi_control():
    return {"source": get_classi_scuola,
            "source_params": {"scuola_id": "VRTF03000V",
                              "api_handler": None},
            "prepare_dict": classe_dict_structure,
            "structure_check": check_classe_dict_structure}


def libro_control():
    return {"source": get_libro,
            "source_params": {"libro_id": 9788823365957,  # None,
                              "api_handler": None},
            "prepare_dict": libro_dict_structure,
            "structure_check": check_libro_dict_structure}


def adozione_control():
    return {"source": get_libri_adottati,
            "source_params": {"classe_id": 559024,  # None,
                              "scuola_id": "VRTF03000V",
                              "api_handler": None},
            "prepare_dict": adozione_dict_structure,
            "structure_check": check_adozione_dict_structure}


def get_controllers():
    return {"scuole": scuola_control(),
            "classi": classi_control(),
            "libri": libro_control(),
            "adozioni": adozione_control()}


def prepare_dict_from_mapping(source_dict, api_keys, table_keys):
    prepared_dict = {}
    for table_key, api_key in zip(table_keys, api_keys):
        prepared_dict[table_key] = source_dict.get(api_key)
    return prepared_dict


def scuola_dict_structure(source_dict, source_params=None):
    api_keys = ["COSCUO", "DESIS", "TIPO_SCUOLA", "NOMSCU", "INDSCU", "CAPSCU", "LOCSCU"]
    table_info_model = table_scuole_model()
    table_keys = [col[0] for col in table_info_model["columns"]]

    prepared_dict = prepare_dict_from_mapping(source_dict, api_keys, table_keys)

    return prepared_dict


def classe_dict_structure(source_dict, source_params=None):
    api_keys = ["CLID", "COSCUO", "ASCO", "CLASSE", "SEZION", "DESCOMB"]
    table_info_model = table_classi_model()
    table_keys = [col[0] for col in table_info_model["columns"]]

    prepared_dict = prepare_dict_from_mapping(source_dict, api_keys, table_keys)

    if "ASCO" not in source_dict:
        prepared_dict["ASCO"] = "2023/2024"

    return prepared_dict


def libro_dict_structure(source_dict, source_params=None):
    table_info_model = table_libri_model()
    table_keys = [col[0] for col in table_info_model["columns"]]

    item_attributes = source_dict.get('ItemAttributes', {})
    detail_page_url = source_dict.get('DetailPageURL')
    large_image = source_dict.get('LargeImage', {}).get('URL')
    offers = (source_dict.get('Offers', {}).get('Offer', {}).get('OfferListing', {}).get('Price', {})
              .get('FormattedPrice'))
    values = [item_attributes.get(column) if column in item_attributes else None for column in table_keys]
    values[0] = values[0] if values[0] else source_params.get('libro_id')
    values[2] = '; '.join(values[2]) if values[2] else None
    values[6] = detail_page_url if detail_page_url else None
    values[7] = large_image if large_image else None
    values[8] = ''.join(c for c in offers if c.isdigit() or c == ',').replace(',', '.') if offers else None
    values[8] = float(values[8]) if values[8] else None

    return dict(zip(table_keys, values))


def adozione_dict_structure(source_dict, source_params=None):
    table_info_model = table_adozioni_model()
    table_keys = [col[0] for col in table_info_model["columns"]]
    values = [source_dict.get(column, None) for column in table_keys]
    for i in range(4, 7):
        if values[i] not in ['Si', 'No']:
            print(f"Cambio il valore '{values[i]}' per il constraint '{table_keys[i]}' "
                  f"dell'adozione nella classe '{values[1]}' per il libro '{values[0]}'")
            values[i] = None
    return dict(zip(table_keys, values))


def check_scuola_dict_structure(source_dict):
    table_info_model = table_scuole_model()
    expected_keys = [col[0] for col in table_info_model["columns"]]
    return all(key in source_dict for key in expected_keys)


def check_classe_dict_structure(source_dict):
    table_info_model = table_classi_model()
    expected_keys = [col[0] for col in table_info_model["columns"]]
    return all(key in source_dict for key in expected_keys)


def check_libro_dict_structure(source_dict):
    table_info_model = table_libri_model()
    expected_keys = [col[0] for col in table_info_model["columns"]]
    return all(key in source_dict for key in expected_keys)


def check_adozione_dict_structure(source_dict):
    table_info_model = table_adozioni_model()
    expected_keys = [col[0] for col in table_info_model["columns"]]
    return all(key in source_dict for key in expected_keys)


def process_dict(table_name, source_params, source_dict, database_handler=None):
    controllers = get_controllers()
    prepare_dict_function = controllers[table_name]["prepare_dict"]
    prepare_dict = prepare_dict_function(source_dict, source_params)
    check_structure_function = controllers[table_name]["structure_check"]
    dati = check_structure_function(prepare_dict)
    # print(f"Dato trattato '{prepare_dict}' con esito: '{dati}'")
    if not dati:
        print(f"Errore: la struttura del dizionario ottenuto dall'API non è corretta per la tabella '{table_name}'.")
        return

    tables_model_structure = tables_model()
    columns = [col[0] for col in tables_model_structure[table_name]["columns"]]
    values = [prepare_dict.get(column, None) for column in columns]

    pk_name = tables_model_structure[table_name]["pk"][0]
    pk_value = prepare_dict[pk_name]

    existing_row = select_row(table_name, pk_name, pk_value, database_handler)

    if existing_row:
        existing_data = dict(existing_row)
        existing_values = [existing_data[column] for column in columns[1:]]
        if existing_values != values[1:]:
            update_row(table_name, columns[1:], values[1:], pk_name, pk_value, database_handler)
            print(f"I dati della tabella '{table_name}' -> '{pk_value}' sono aggiornati.")
        else:
            print(f"I dati della tabella '{table_name}' -> '{pk_value}' sono già aggiornati.")
    else:
        insert_row(table_name, columns, values, database_handler)
        print(f"Nuova riga inserita nella tabella '{table_name}' -> '{pk_value}'.")


def insert_data_from_api(table_name, database_api_handler=None, source_param=None, source_dict_list=None):
    database_api_handler = validate_database_api_handler(database_api_handler)
    controllers = get_controllers()
    source_function = controllers[table_name]["source"]
    source_params = controllers[table_name]["source_params"]
    source_params["api_handler"] = database_api_handler.get_api_handler()

    if source_param is not None and isinstance(source_param, dict):
        source_params.update({k: v for k, v in source_param.items() if v is not None})

    if None in source_params.values():
        get_dynamic_parameter_value(table_name, database_api_handler, source_params)

    if not source_dict_list:
        source_dict_list = fetch_data_from_source(source_function, source_params)

    database_handler = database_api_handler.get_db_handler()

    for source_dict in source_dict_list:
        process_dict(table_name, source_params, source_dict, database_handler)

    database_api_handler.commit_connection()


def get_dynamic_parameter_value(table_name, database_api_handler=None, source_param=None):
    database_api_handler = validate_database_api_handler(database_api_handler)
    database_handler = database_api_handler.get_db_handler()
    if table_name == "adozioni":
        classe_ids = retrieve_classe_ids(source_param["scuola_id"], database_handler)
        print(f"Classi da recuperare: '{classe_ids}'")
        for classe_id in classe_ids:
            adozione_classe(classe_id, database_api_handler, source_param)
    else:
        print("Errore: Parametri della sorgente non completamente specificati.")
        return


def adozione_classe(classe_id, db_api_handler=None, source_param=None):
    api_handler = db_api_handler.get_api_handler()
    source_param_copy = source_param.copy()
    source_param_copy["classe_id"] = classe_id
    source_param_copy["api_handler"] = api_handler
    source_dict_list = retry_session(get_libri_adottati, source_param_copy)

    for adozione in source_dict_list:
        book_ids = adozione.get('ISBN')
        new_param = {"libro_id": book_ids, "api_handler": api_handler}

        db_api_handler.acquire_semaphore()
        try:
            insert_data_from_api("libri", db_api_handler, new_param)
        finally:
            db_api_handler.release_semaphore()

    db_api_handler.acquire_semaphore()
    try:
        insert_data_from_api("adozioni", db_api_handler, source_param_copy, source_dict_list)
    finally:
        db_api_handler.release_semaphore()


def worker(classe_id, source_param=None):
    db_api_handler = DatabaseAPIHandler()  # Ogni worker crea la propria istanza

    db_api_handler.acquire_semaphore()
    try:
        adozione_classe(classe_id, db_api_handler, source_param)
    finally:
        db_api_handler.release_semaphore()

    db_api_handler.close_connection()  # Chiusura della connessione al termine del lavoro
