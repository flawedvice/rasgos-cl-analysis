from typing import List, Dict
import requests as req
from datetime import datetime
import logging as log
import pandas as pd
import json
import os


def prepare():
    """
    Prepares environment to download all of the needed data from Herbario Digital's public API.
    This is a long process, so we want to handle it carefully.
    This function performs the following tasks:

    - Creates needed directories for data download and error logging.
    - Creates error logger.
    - Downloads Rasgos-CL Database into data files for future filtering.
    """

    # Directories creation
    dirs = ["data", os.path.join("data", "temp"), "errors"]
    for dir in dirs:
        if not os.path.exists(dir):
            os.mkdir(dir)

    # Logging configuration
    today = datetime.today()
    error_log_file = os.path.join("errors", f"{today}.log")
    formatter = "%(asctime)s|%(levelname)s|%(funcName)s at line %(lineno)d: %(message)s"
    log.basicConfig(
        filename=error_log_file,
        datefmt="%Y%m%d %H:%M:%S",
        format=formatter,
        filemode="a+",
    )
    log.root.setLevel(log.ERROR)

    # Pre-requisite data download
    traits_url = "https://raw.githubusercontent.com/dylancraven/Rasgos-CL/main/Data/RasgosCL_spp_names_clean.csv"
    try:
        traits_df = pd.read_csv(traits_url)
        filepath = os.path.join("data", "species_names.csv")
        traits_df.to_csv(filepath)
    except Exception as err:
        print(f"Error when downloading Rasgos-CL species data: {err}")
        log.error(err)


def save_temp(filename: str, data: any):
    """
    Saves json data into temporary location.

    Returns file location.
    """
    filepath = os.path.join("data", "temp", filename)
    try:
        with open(filepath, "w") as file:
            json.dump(data, file)
    except ValueError as value_error:
        print(f"Value error when saving data into {filename}")
        log.error(value_error)
    except TypeError as type_error:
        print(f"Type error when saving data into {filename}")
        log.error(type_error)

    return filepath


def clean_empty_logs():
    """
    Deletes empty log files.
    """
    logs = os.listdir("errors")
    for log in logs:
        filepath = os.path.join("errors", log)
        with open(filepath, "r") as logfile:
            if not len(logfile.readlines()):
                os.remove(filepath)


def get_all(start_at=1) -> List[Dict]:
    """
    Retrieves every specie's scientific_name and id available at Herbario Digital's public API.
    """
    print("Retrieving species list")
    base_url = "https://api.herbariodigital.cl/species_list/?format=json"
    page = start_at - 1

    species_list = list()

    has_data = True
    while has_data:
        results = list()
        page += 1
        url = f"{base_url}&page={page}"

        print(f"Retrieving page {page}...")
        try:
            res = req.get(url)
        except req.ConnectionError as conn_err:
            print(f"Error when accessing the API: {conn_err}")
            log.error(conn_err)
            break

        if res.status_code != 200:
            print(f"Non-ok status code at page {page}")
            continue

        try:
            json_data = res.json()
        except req.JSONDecodeError as decode_error:
            print(f"Error when decoding json at page {page}")
            log.error(decode_error)
            break

        results = json_data.get("results")
        if not results or not len(results):
            print("No more data. Closing process.")
            has_data = False
            break

        species_list.extend(
            [
                {
                    "id": specie.get("id"),
                    "scientific_name": specie.get("scientific_name"),
                }
                for specie in results
            ]
        )

    return species_list


def get_accepted_names() -> List[str]:
    """
    Reads Rasgos-CL database and returns a list of accepted names.
    """
    print("Getting species' scientific names from Rasgos-CL")
    db_path = os.path.join("data", "species_names.csv")
    if not os.path.exists:
        print("Database has not been downloaded! Remember to call `prepare()`")
        return
    df = pd.read_csv(db_path)
    return df["accepted_full_name"].to_list()


def filter_species(herbario_species: List[Dict], name_list: List[str]) -> List[Dict]:
    """
    Filters collected species depending if their scientific name is in the name_list.

    Returns a list of dictionaries `{ "herbario_id": int, "scientific_name": str }`.
    """
    print("Filtering species")
    accepted_species = list()
    for specie in herbario_species:
        scientific_name = specie.get("scientific_name")
        if scientific_name and scientific_name in name_list:
            accepted_species.append(
                {
                    "id": specie.get("id"),
                    "scientific_name": scientific_name,
                }
            )
    return accepted_species


def get_accepted_species(herbario_species: List[Dict]) -> List[Dict]:
    """
    Retrieves specific species' data available at Herbario Digital's public API.
    """
    print("Retrieving accepted species")
    base_url = "https://api.herbariodigital.cl/species/"

    species_list = list()

    for idx, specie in enumerate(herbario_species):
        id = specie.get("id")
        scientific_name = specie.get("scientific_name")
        if not id:
            msg = f"No `id` available for {scientific_name}"
            print(msg)
            log.error(msg)
            continue

        url = f"{base_url}{id}/?format=json&lang=en"
        print(f"Retrieving specie {idx+1} of {len(herbario_species)}")

        try:
            res = req.get(url)
        except req.ConnectionError as conn_err:
            print(f"error when accessing the API: {conn_err}")
            log.error(conn_err)
            break

        if not res.ok:
            msg = f"Non-ok status code at specie {id}: [{res.status_code}] {res.reason}"
            print(msg)
            log.error(msg)
            continue

        try:
            json_data = res.json()
        except req.JSONDecodeError as decode_error:
            print(f"Error when decoding json at specie with id {id}")
            log.error(decode_error)
            break

        species_list.append(json_data)

    return species_list


def simplify_data(herbario_species: List[Dict]) -> pd.DataFrame:
    """
    Filters obtained data and transforms it into a simplified version ready to be used by pandas.

    Parameters
    ----------
    herbario_species   :   List[Dict]
        List of filtered and accepted species.

    Returns
    -------
    pd.DataFrame
        a pd.DataFrame of simplified data for analysis.
    """
    conservation_states = [
        "Not Evaluated (NE)",
        "Data Deficient (DD)",
        "Least Concern (LC)",
        "Conservation Dependent (CD)",
        "Near Threatened (NT)",
        "Almost Threatened (NT)",
        "Vulnerable (VU)",
        "Endangered (EN)",
        "Critically Endangered (CR)",
        "Extinct in the Wild (EW)",
        "Extinct (EX)",
    ]

    regions = {
        "Araucania Region": "Araucanía",
        "Maule Region": "Maule",
        "Atacama Region": "Atacama",
        "Antofagasta Region": "Antofagasta",
        "Juan Fernández Archipelago": "Juan Fernández",
        "Tarapaca Region": "Tarapacá",
        "Santiago Metropolitan Region": "Metropolitana",
        "Liberator General Bernardo O'Higgins Region": "Libertador Bernardo O'Higgins",
        "Arica and Parinacota Region": "Arica y Parinacota",
        "River Region": "Los Ríos",
        "Ñuble Region": "Ñuble",
        "Coquimbo Region": "Coquimbo",
        "Los Lagos Region": "Los Lagos",
        "Magallanes and Chilean Antarctic Region": "Magallanes",
        "Bio Bio Region": "Bío-Bío",
        "Valparaiso Region": "Valparaíso",
        "Region of Aysén del General Carlos Ibáñez del Campo": "Aysén",
    }

    simplified = []
    for specie in herbario_species:
        simplified_specie = {
            "id": specie.get("id"),
            "scientific_name": specie.get("scientific_name"),
            "habit": specie.get("habit"),
            "status": specie.get("status"),
            "conservation_state": conservation_states[0],
        }
        pre_conservation_state = specie.get("conservation_state")
        if len(pre_conservation_state) >= 1:
            simplified_specie["conservation_state"] = sorted(
                pre_conservation_state,
                key=lambda state: conservation_states.index(state),
            )[0]

        pre_regions = [region.get("name") for region in specie.get("region")]
        for raw_region, common_region in regions.items():

            if raw_region in pre_regions:
                simplified_specie[common_region] = 1
            else:
                simplified_specie[common_region] = 0

        simplified.append(simplified_specie)

    return pd.DataFrame(simplified)


def pipeline(clean_logs=True, clean_temp=False) -> pd.DataFrame:
    """
    Custom data pipeline for obtaining data from HerbarioDigital.
    Attempts to read already downloaded data from `data/temp/`.
    Falls back to latest available file until it find the latest step.
    If no file is found, attempts to download data directly from the API.

    Parameters
    ----------
    clean_empty_logs    :   bool, optional
        Deletes every empty log file at `errors/`.
        (default is True)
    clean_temp  :   bool, optional
        Deletes every json file at `data/temp/`.
        (default is False)

    Returns
    -------
    pd.DataFrame
        a pd.DataFrame with data of species from HerbarioDigital's public API.
    """
    prepare()

    all_filename = "herbario_species_all.json"
    all_path = os.path.join("data", "temp", all_filename)

    filtered_filename = "herbario_species_filtered.json"
    filtered_path = os.path.join("data", "temp", filtered_filename)

    accepted_filename = "herbario_species_accepted.json"
    accepted_path = os.path.join("data", "temp", accepted_filename)

    species_filename = "herbario_species.csv"
    species_path = os.path.join("data", species_filename)

    # Get final json file
    if os.path.exists(species_path):
        print("Reusing last species file")
        herbario_species = pd.read_csv(species_path)

    elif os.path.exists(accepted_path):
        print("Reusing last accepted species file")
        with open(accepted_path, "r") as file:
            herbario_species_accepted = json.load(file)
        herbario_species = simplify_data(herbario_species_accepted)
        herbario_species.to_csv(species_path)

    elif os.path.exists(filtered_path):
        print("Reusing last filtered species file")
        with open(filtered_path, "r") as file:
            herbario_species_filtered = json.load(file)
        herbario_species_accepted = get_accepted_species(herbario_species_filtered)
        save_temp("herbario_species_accepted.json", herbario_species_accepted)
        herbario_species = simplify_data(herbario_species_accepted)
        herbario_species.to_csv(species_path)

    elif os.path.exists(all_path):
        print("Reusing last species list file")
        with open(all_path, "r") as file:
            herbario_species_all = json.load(file)

        accepted_names = get_accepted_names()
        herbario_species_filtered = filter_species(herbario_species_all, accepted_names)
        save_temp("herbario_species_filtered.json", herbario_species_filtered)
        herbario_species_accepted = get_accepted_species(herbario_species_filtered)
        save_temp("herbario_species_accepted.json", herbario_species_accepted)
        herbario_species = simplify_data(herbario_species_accepted)
        herbario_species.to_csv(species_path)

    else:
        print("No file to reuse, downloading from source")
        herbario_species_all = get_all()
        save_temp("herbario_species_all.json", herbario_species_all)

        accepted_names = get_accepted_names()
        herbario_species_filtered = filter_species(herbario_species_all, accepted_names)
        save_temp("herbario_species_filtered.json", herbario_species_filtered)
        herbario_species_accepted = get_accepted_species(herbario_species_filtered)
        save_temp("herbario_species_accepted.json", herbario_species_accepted)
        herbario_species = simplify_data(herbario_species_accepted)
        herbario_species.to_csv(species_path)

    if clean_logs:
        clean_empty_logs()

    if clean_temp:
        temp_dir = os.path.join("data", "temp")
        temp_files = os.listdir(temp_dir)
        for file in temp_files:
            filepath = os.path.join(temp_dir, file)
            os.remove(filepath)

    return herbario_species


def _main():
    pipeline(clean_logs=True, clean_temp=False)


# Run main if file is executed independently
if __name__ == "__main__":
    _main()
