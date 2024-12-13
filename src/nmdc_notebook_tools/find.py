# -*- coding: utf-8 -*-
from api import NMDClient
import requests
import urllib.parse
from data_processing import convert_to_df
from utils import get_id_list, split_list, string_mongo_list


def find_study_by_id(study_id: str):
    """
    Get a study from the NMDC API by its id.
    params:
        study_id: str
            The id of the study to query.
    """
    api_client = NMDClient()

    url = f"{api_client.base_url}/study/{study_id}"
    # get the reponse
    response = requests.get(url)
    # check it came back with OK
    if response.status_code != 200:
        return (response.status_code, "There was an error.")
    results = response.json()

    return convert_to_df(results)


def _combine_results(resp: dict) -> dict:
    pass


def find_study_by_attribute(attribute_name, attribute_value):
    """
    Get a study from the NMDC API by its name. Studies can be filter based on their attributes found https://microbiomedata.github.io/nmdc-schema/Study/.
    params:
        study_name: str
            The name of the study to query.
        attribute_name: str
            The name of the attribute to filter by.
        attribute_value: str
            The value of the attribute to filter by.
    """
    api_client = NMDClient()

    filter = f"{attribute_name}.search:{attribute_value}"

    # Encode the filter for use in the URL
    encoded_filter = urllib.parse.quote(filter)

    url = f"{api_client.base_url}/studies?filter={encoded_filter}"
    # get the reponse
    response = requests.get(url)
    # check it came back with OK
    if response.status_code != 200:
        return (response.status_code, "There was an error.")
    results = response.json()["results"]

    return convert_to_df(results)


if __name__ == "__main__":
    find_study_by_attribute("id", "nmdc:sty-11-8fb6t785")
