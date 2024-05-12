import requests
from time import sleep


class APIHandler:
    def __init__(self):
        self._url = "https://www.adozionilibriscolastici.it"
        self._session = None

    def get_api_session(self):
        if self._session is None:
            print("APIHandler: Nuova Sessione!")
            self._session = self.get_session()
        else:
            print("APIHandler: Riutilizzo Sessione!")
        return self._session

    def get_session(self):
        """
        Create and return a session object.
        """
        session = requests.Session()
        session.get(self._url)
        return session


def validate_api_handler(api_handler=None):
    if api_handler is None and not isinstance(api_handler, APIHandler):
        print("APIHandler: Creazione di una nuova istanza!")
        api_handler = APIHandler()
    else:
        print("APIHandler: Riutilizzo dell'istanza!")
    return api_handler


def get_data(url, api_handler=None):
    """
    Fetch data from the specified URL using the provided session.
    """
    list_dict = None
    api_handler = validate_api_handler(api_handler)
    session = api_handler.get_api_session()
    response = session.get(url)
    if response.status_code == 200:
        list_dict = response.json()
    return list_dict


def get_public_ip():
    """
    Fetches the public IP address using the ipify service.
    """
    url = "https://api.ipify.org"
    session = requests.Session()
    response = session.get(url)
    public_ip = None
    if response.status_code == 200:
        public_ip = response.text
    return public_ip


def get_regioni():
    return {"Piemonte": "01", "Valle d'Aosta": "02",
            "Lombardia": "03", "Trentino Alto Adige": "04",
            "Veneto": "05", "Friuli Venezia Giulia": "06",
            "Liguria": "07", "Emilia Romagna": "08",
            "Toscana": "09", "Umbria": "10",
            "Marche": "11", "Lazio": "12",
            "Abruzzo": "13", "Molise": "14",
            "Campania": "15", "Puglia": "16",
            "Basilicata": "17", "Calabria": "18",
            "Sicilia": "19", "Sardegna": "20"}


def get_province(region_id="05", api_handler=None):
    """
    Fetch province data for the given region ID.
    Output:
    [{'ID': 'BL', 'VALUE': 'Belluno'}, {...}]
    """
    api_url = "https://www.adozionilibriscolastici.it/v1/regioni"
    url = f"{api_url}/{region_id}"
    return get_data(url, api_handler)


def get_comuni(province_id="VR", api_handler=None):
    """
    Fetch municipalities data for the given province ID.
    Output:
    [{'ID': 'Affi', 'VALUE': 'Affi'}, {...}]
    """
    api_url = "https://www.adozionilibriscolastici.it/v1/province"
    url = f"{api_url}/{province_id}"
    return get_data(url, api_handler)


def get_gradi(comune_id="Verona", api_handler=None):
    """
    Fetch grades data for the given municipality ID.
    Output:
    [{'ID': 0, 'VALUE': 'SCUOLA PRIMARIA'}, {...}]
    """
    api_url = "https://www.adozionilibriscolastici.it/v1/comuni"
    url = f"{api_url}/{comune_id}"
    return get_data(url, api_handler)


def get_scuole_grado(comune_id="Verona", grado_id=2, api_handler=None):
    """
    Fetch schools data for the given municipality and grade IDs.
    Output:
    [{'CAPSCU': '37121', 'COSCUO': 'VRPS04000B',
    'DESIS': 'SCUOLA SECONDARIA DI II GRADO', 'INDSCU': 'VIA DON G. BERTONI N. 3/B',
    'LOCSCU': 'Verona', 'NOMSCU': '"ANGELO MESSEDAGLIA"', 'FRZSCU': None,
    'TIPO_SCUOLA': 'LICEO SCIENTIFICO', 'GRADO': 2}, {...}]
    """
    api_url = "https://www.adozionilibriscolastici.it/v1/scuole"
    url = f"{api_url}?locId={comune_id}&grado={grado_id}"
    return get_data(url, api_handler)


def get_classi_scuola(scuola_id="VRTF03000V", api_handler=None):
    """
    Fetch classes data for the given school ID.
    Output:
    [{'CLASSE': '1', 'CLID': 559024, 'COSCUO': 'VRTF03000V',
    'DESCOMB': 'INFORMATICA E TELECOMUNICAZIONI - BIENNIO COMUNE',
    'DESIS': 'SCUOLA SECONDARIA DI II GRADO', 'SEZION': 'AI', 'CODSPR': 'IT13',
    'CODSPC': None, 'TIPSCU': 'NO', 'ABTEST_NEWIF': 1, 'ABTEST_ENABLED': 1,
    'DES_COMB_CNT': 2}, {...}]
    """
    api_url = "https://www.adozionilibriscolastici.it/v1/classi"
    url = f"{api_url}/{scuola_id}"
    return get_data(url, api_handler)


def get_libri_adottati(classe_id, scuola_id="VRTF03000V", api_handler=None):
    """
    Fetch adopted books for the given class ID and school ID.
    Output:
    [{'AUTORI': 'CORDIOLI DORIANO  ', 'CLASSE': '1', 'CLID': 559024,
    'CONSIGLIATO': 'No', 'COSCUO': 'VRTF03000V', 'DA_ACQUISTARE': 'Si',
    'DESC_DISCIPLINA': 'CHIMICA', 'EDITORE': 'TRAMONTANA', 'ISBN': '9788823365957',
    'NUOVA_ADOZIONE': 'No', 'ORD': 0, 'SEZIONE': 'AI',
    'SOTTOTITOLO': 'VOLUME UNICO PER IL BIENNIO',
    'TITOLO': 'CHIMICA PRATICA - LIBRO MISTO CON LIBRO DIGITALE',
    'SHOW_USED': 0, 'SHOW_PRIME_NOW_EAN': 0, 'SHOW_PRIME_NOW_COSCUO': 0,
    'DIGITALE': 0}, {...}]
    """
    api_url = f"https://www.adozionilibriscolastici.it/v1/libri"
    url = f"{api_url}/{classe_id}/{scuola_id}"
    return get_data(url, api_handler)


def get_libro(libro_id, api_handler=None):
    """
    Fetch book details by libro ID. Output: [{'ASIN': '8823365953', 'DetailPageURL':
    'https://www.amazon.it/dp/8823365953?tag=adozilibris0f-21&linkCode=ogi&th=1&psc=1', 'Images': {'Primary': {
    'Small': {}, 'Medium': {}, 'Large': {}}}, 'ItemInfo': {'ByLineInfo': {'Contributors': [{'Locale': 'it_IT',
    'Name': 'Cordioli, Doriano', 'Role': 'Autore'}], 'Manufacturer': {'Label': 'Manufacturer', 'Locale': 'it_IT'}},
    'Classifications': {'Binding': {'DisplayValue': 'Copertina flessibile', 'Label': 'Binding', 'Locale': 'it_IT'},
    'ProductGroup': {'DisplayValue': 'Libro', 'Label': 'ProductGroup', 'Locale': 'it_IT'}}, 'ContentInfo': {
    'Languages': {'DisplayValues': [{'DisplayValue': 'Italiano', 'Type': 'Pubblicato'}, {'DisplayValue': 'Italiano',
    'Type': 'Lingua originale'}], 'Label': 'Language', 'Locale': 'it_IT'}, 'PagesCount': {'DisplayValue': 976,
    'Label': 'NumberOfPages', 'Locale': 'en_US'}, 'PublicationDate': {'Label': 'PublicationDate', 'Locale':
    'en_US'}}, 'ExternalIds': {'EANs': {'DisplayValues': [None], 'Label': 'EAN', 'Locale': 'en_US'},
    'ISBNs': {'DisplayValues': ['8823365953'], 'Label': 'ISBN', 'Locale': 'en_US'}}, 'ProductInfo': {'ReleaseDate': {
    'Label': 'ReleaseDate', 'Locale': 'en_US'}, 'UnitCount': {'DisplayValue': 1, 'Label': 'NumberOfItems',
    'Locale': 'en_US'}}, 'Title': {'Label': 'Title', 'Locale': 'it_IT'}}, 'Offers': {'Listings': [None], 'Summaries':
    [None, {'Condition': {'Value': 'Used'}, 'HighestPrice': {'Amount': 21.3, 'Currency': 'EUR', 'DisplayAmount': '21,
    30\xa0€'}, 'LowestPrice': {'Amount': 19.9, 'Currency': 'EUR', 'DisplayAmount': '19,90\xa0€'}, 'OfferCount': 5}],
    'Offer': {'OfferListing': {'Availability': 'Disponibilità immediata', 'Condition': {'Value': 'New'},
    'DeliveryInfo': {'IsAmazonFulfilled': True, 'IsFreeShippingEligible': True, 'IsPrimeEligible': True},
    'Id': 'gYbdzIUn54wf85n7LARXOdesG4AD1v9rh892qbmNPgx5t41T2ej3owVgIADTcnTy01nV%2BRZd
    %2FkrgOKnjLGaH8hKGWxKs3cNgxeVJZ1WmQKiGcZhiFMKwzV7mWnjBty0JmdnFHqgjzIS5SypPuroaXw%3D%3D', 'IsBuyBoxWinner': True,
    'MerchantInfo': {'Id': 'A11IL2PNWYJU7H'}, 'Price': {'Amount': '3690', 'Currency': 'EUR', 'FormattedPrice': '36,
    90\xa0€'}, 'ViolatesMAP': False, 'AvailabilityAttributes': {'AvailabilityType': 'Now'}}, 'Merchant': {'Name':
    'Amazon.it'}, 'OfferAttributes': {'Condition': 'New'}}, 'TotalOffers': 1}, 'SmallImage': {'URL':
    'https://m.media-amazon.com/images/I/51tLkKg2kML._SL75_.jpg', 'Height': {'_': 75}, 'Width': {'_': 55}},
    'MediumImage': {'URL': 'https://m.media-amazon.com/images/I/51tLkKg2kML._SL160_.jpg', 'Height': {'_': 160},
    'Width': {'_': 117}}, 'LargeImage': {'URL': 'https://m.media-amazon.com/images/I/51tLkKg2kML._SL500_.jpg',
    'Height': {'_': 500}, 'Width': {'_': 368}}, 'OfferSummary': {'Condition': {}, 'HighestPrice': {'Currency':
    'EUR'}, 'LowestPrice': {'Amount': 36.9, 'Currency': 'EUR', 'DisplayAmount': '36,90\xa0€'}, 'TotalNew': 4},
    'ItemAttributes': {'ReleaseDate': '2021-09-15T00:00:01Z', 'PublicationDate': '2021-09-15T00:00:01Z',
    'Title': 'Chimica pratica. Vol. unico. Per il biennio delle Scuole superiori. Con e-book. Con espansione online',
    'Publisher': 'Tramontana', 'ListPrice': {'Amount': '4190', 'FormattedPrice': '41,90\xa0€'},
    'EAN': '9788823365957', 'Label': 'Tramontana', 'Author': ['Cordioli, Doriano']}}]
    """
    api_url = f"https://www.adozionilibriscolastici.it/v1/lookup"
    url = f"{api_url}/{libro_id}"
    return get_data(url, api_handler)


def get_scuola(scuola_id="VRTF03000V", comune_id="Verona", grado_id=2, api_handler=None):
    scuole_grado_comune_dict = get_scuole_grado(comune_id, grado_id, api_handler)
    scuola_key = "COSCUO"
    return [get_dict_from_id(scuole_grado_comune_dict, scuola_id, scuola_key)]


def fetch_data_from_source(source_function, source_params):
    if isinstance(source_params, dict):
        source_dict_list = source_function(**source_params)
    else:
        source_dict_list = source_function(source_params)

    print(f"APIHandler: '{source_function.__name__}' -> '{source_params}': '{source_dict_list}'")

    return source_dict_list if source_dict_list is not None else [{}]


def retry_session(source_function, source_params):
    while True:
        source_dict_list = fetch_data_from_source(source_function, source_params)
        if len(source_dict_list) == 1 and not source_dict_list[0]:
            public_ip = get_public_ip()
            wait = 5
            print(f"APIHandler: Accesso API limitato. Attendere {wait} secondi e ottenere una nuova sessione."
                  f"\nSe il problema persiste, potrebbe essere necessario cambiare l'indirizzo IP ({public_ip}).")
            sleep(wait)
            source_params["api_handler"] = APIHandler()
        else:
            break
    return source_dict_list


def get_dict_from_id(data_list, search_values, key_id='ID'):
    """
    Search for the dictionary corresponding to the provided ID in the given data list.
    """
    id_dict = None

    for item in data_list:
        if item.get(key_id) == search_values:
            id_dict = item
            break
    return id_dict


def get_id_from_dict(data_list, search_values, key_id='ID'):
    """
    Search for the ID corresponding to the provided value in the given data list.
    """
    id_value = None

    if not isinstance(search_values, dict):
        search_values = {'VALUE': search_values}

    for item in data_list:
        if all(item.get(key, '') == value for key, value in search_values.items()):
            id_value = item[key_id]
            break
    return id_value


def get_all_ids_from_dict(data_list, key_id='ID'):
    """
    Extracts all IDs corresponding to the provided key from the given list of dictionaries.
    """
    id_list = []
    for item in data_list:
        if key_id in item:
            id_list.append(item[key_id])
    return id_list


def print_value_info(value_name, value_id):
    """
    Print information about the value and its corresponding ID.
    """
    if value_id:
        print(f"Valore ID per '{value_name}': {value_id}")
    else:
        print(f"{value_name} non trovato.")


def print_info_section(title, data):
    """
    Print information section with a title.
    """
    print(f"\n{title}:")
    print(data)


def main():
    api = APIHandler()
    regioni_dict = get_regioni()
    print_info_section("Regioni in Italia", regioni_dict)

    regione_value = "Veneto"
    regione_id = regioni_dict.get(regione_value)
    print_value_info(regione_value, regione_id)

    province_regione_dict = get_province(regione_id, api)
    print_info_section(f"Province della regione '{regione_value}' ",
                       province_regione_dict)

    provincia_value = "Verona"
    provincia_id = get_id_from_dict(province_regione_dict, provincia_value)
    print_value_info(provincia_value, provincia_id)

    comuni_provincia_dict = get_comuni(provincia_id, api)
    print_info_section(f"Comuni della provincia di '{provincia_value}' ",
                       comuni_provincia_dict)

    comune_value = "Verona"
    comune_id = get_id_from_dict(comuni_provincia_dict, comune_value)
    print_value_info(comune_value, comune_id)

    gradi_comune_dict = get_gradi(comune_id, api)
    print_info_section(f"Gradi scolastici presenti nel comune di '{comune_value}' ",
                       gradi_comune_dict)

    grado_value = "SCUOLA SECONDARIA DI II GRADO"
    grado_id = get_id_from_dict(gradi_comune_dict, grado_value)
    print_value_info(grado_value, grado_id)

    scuole_grado_comune_dict = get_scuole_grado(comune_id, grado_id, api)
    print_info_section(f"Scuole appartenenti al grado '{grado_value}' "
                       f"presenti nel comune di '{comune_value}'",
                       scuole_grado_comune_dict)

    scuola_value = '"GUGLIELMO MARCONI"'
    search_values = {"NOMSCU": scuola_value}
    scuola_key_id = "COSCUO"
    scuola_id = get_id_from_dict(scuole_grado_comune_dict, search_values, scuola_key_id)
    print_value_info(scuola_value, scuola_id)

    classi_scuola_dict = get_classi_scuola(scuola_id, api)
    print_info_section(f"Classi presenti nella scuola '{scuola_value}' ",
                       classi_scuola_dict)

    classe_value = ["1", "AI"]
    search_values = {"CLASSE": classe_value[0], "SEZION": classe_value[1]}
    classe_key_id = "CLID"
    classe_id = get_id_from_dict(classi_scuola_dict, search_values, classe_key_id)
    print_value_info(classe_value, classe_id)

    libri_adottati_classe_dict = get_libri_adottati(classe_id, scuola_id, api)
    print_info_section(f"Libri adottati dalla classe '{classe_value[0]}' "
                       f"sezione '{classe_value[1]}' "
                       f"della scuola '{scuola_value}' ",
                       libri_adottati_classe_dict)

    libro_value = "CHIMICA PRATICA - LIBRO MISTO CON LIBRO DIGITALE"
    search_values = {"TITOLO": libro_value}
    libro_key_id = "ISBN"
    libro_id = get_id_from_dict(libri_adottati_classe_dict, search_values, libro_key_id)
    print_value_info(libro_value, libro_id)

    libro_dict = get_libro(libro_id, api)
    print_info_section(f"Dettaglio del libro con titolo '{libro_value}' ",
                       libro_dict)

    # Estrai tutti gli ID dalla lista di classi
    all_classe_ids = get_all_ids_from_dict(classi_scuola_dict, classe_key_id)
    print_info_section(f"Lista degli ID delle classi nella scuola '{scuola_value}' ",
                       all_classe_ids)

    # Estrai tutti gli ID dalla lista dei libri adottati in classe
    all_libro_ids = get_all_ids_from_dict(libri_adottati_classe_dict, libro_key_id)
    print_info_section(f"Lista degli ID dei libri nella classe '{classe_value[0]}'"
                       f"sezione '{classe_value[1]}' "
                       f"della scuola '{scuola_value}' ",
                       all_libro_ids)


if __name__ == '__main__':
    main()
