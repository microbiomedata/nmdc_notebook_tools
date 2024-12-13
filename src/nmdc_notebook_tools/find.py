# -*- coding: utf-8 -*-
from api import NMDClient
import requests
from data_processing import convert_to_df
from utils import get_id_list, split_list, string_mongo_list


def get_study_by_id(study_id: str):
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
    results = response.json()["resources"]

    return convert_to_df(results)
