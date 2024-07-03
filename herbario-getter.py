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


def get_all(start_at=1) -> List[Dict]:
    """
    Retrieves every specie's scientific_name and id available at Herbario Digital's public API.
    """
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

    accepted_species = list()
    for specie in herbario_species:
        if specie and specie in name_list:
            accepted_species.append(
                {
                    "herbario_id": specie.get("id"),
                    "scientific_name": specie.get("scientific_name"),
                }
            )
    return accepted_species


def get_accepted_species(herbario_species: List[Dict]) -> List[Dict]:
    """
    Retrieves specific species' data available at Herbario Digital's public API.
    """
    base_url = "https://api.herbariodigital.cl/species/"

    species_list = list()

    for specie in herbario_species:
        id = herbario_species.get("id")
        scientific_name = herbario_species.get("scientific_name")
        if not id:
            msg = f"No `id` available for {scientific_name}"
            print(msg)
            log.error(msg)
            continue

        url = f"{base_url}{id}/?format=json&lang=en"
        print(f"retrieving specie {id}")

        try:
            res = req.get(url)
        except req.ConnectionError as conn_err:
            print(f"error when accessing the API: {conn_err}")
            log.error(conn_err)
            break

        if res.status_code != 200:
            print(f"Non-ok status code at specie with id {id}")
            continue

        try:
            json_data = res.json()
        except req.JSONDecodeError as decode_error:
            print(f"Error when decoding json at specie with id {id}")
            log.error(decode_error)
            break

        species_list.append(json_data)

    return species_list


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


def main():
    prepare()

    herbario_species_all = get_all()
    temp_get_all = save_temp("herbario_species_all.json", herbario_species_all)

    accepted_names = get_accepted_names()
    herbario_species_filtered = filter_species(herbario_species_all, accepted_names)
    temp_filtered = save_temp(
        "herbario_species_filtered.json", herbario_species_filtered
    )

    herbario_species_accepted = get_accepted_species(herbario_species_filtered)
    temp_accepted = save_temp(
        "herbario_species_accepted.json", herbario_species_accepted
    )

    clean_empty_logs()


# Run main if file is executed independently
if __name__ == "__main__":
    main()
