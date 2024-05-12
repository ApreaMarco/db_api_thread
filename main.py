from threading import Thread
from utils.time_utils import measureTime, measureTimeString
from control.db_api_control import DatabaseAPIHandler, tables_model, insert_data_from_api, get_controllers, worker
from model.library import create_tables, retrieve_classe_ids


def main():
    tupleTime = measureTime()
    db_api_handler = DatabaseAPIHandler()
    db_handler = db_api_handler.get_db_handler()

    tables_model_structure = tables_model()
    create_tables(tables_model_structure, db_handler)

    for table_name in ["scuole", "classi"]:
        insert_data_from_api(table_name, db_api_handler)

    controllers = get_controllers()
    source_param = controllers[table_name]["source_params"]
    classe_ids = retrieve_classe_ids(source_param["scuola_id"], db_handler)
    print(f"Classi da recuperare: '{classe_ids}'")

    num_threads = 15

    threads = []

    for classe_id in classe_ids:
        thread = Thread(target=worker, args=(classe_id, source_param))
        thread.start()
        print(f"Partenza Thread: {thread.ident}")
        threads.append(thread)

        if len(threads) >= num_threads:
            for thread in threads:
                thread.join()
            threads = []

    for thread in threads:
        thread.join()

    db_handler.close_connection()
    print(f"time: {measureTimeString(tupleTime)}")


if __name__ == "__main__":
    main()
