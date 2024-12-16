# -*- coding: utf-8 -*-
from api import NMDClient
import requests
import urllib.parse
from data_processing import DataProcessing
from utils import Utils


class Find:
    def __init__(self):
        pass

    def find_study_by_id(self, study_id: str):
        """
        Get a study from the NMDC API by its id.
        params:
            study_id: str
                The id of the study to query.
        """
        api_client = NMDClient()
        dp = DataProcessing()
        url = f"{api_client.base_url}/study/{study_id}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()

        return dp.convert_to_df(results)

    def find_study_by_attribute(self, attribute_name, attribute_value, page_size=25):
        """
        Get a study from the NMDC API by its name. Studies can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Study/.
        params:
            study_name: str
                The name of the study to query.
            attribute_name: str
                The name of the attribute to filter by.
            attribute_value: str
                The value of the attribute to filter by.
            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()
        dp = DataProcessing()
        filter = f"{attribute_name}.search:{attribute_value}"

        # Encode the filter for use in the URL
        encoded_filter = urllib.parse.quote(filter)

        url = f"{api_client.base_url}/studies?filter={encoded_filter}&page_size={page_size}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()["results"]
        return dp.convert_to_df(results)


if __name__ == "__main__":
    find = Find()
    studies = find.find_study_by_attribute(
        attribute_name="id", attribute_value="nmdc:sty"
    )
    print(studies)
