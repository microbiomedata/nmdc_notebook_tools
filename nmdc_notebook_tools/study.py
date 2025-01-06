# -*- coding: utf-8 -*-
from nmdc_notebook_tools.api import NMDClient
import requests
import urllib.parse
from typing import List, Dict
from nmdc_notebook_tools.data_processing import DataProcessing
from nmdc_notebook_tools.utils import Utils


class Study:
    def __init__(self):
        pass

    def find_study_by_id(self, study_id: str) -> Dict:
        """
        Get a study from the NMDC API by its id.
        params:
            study_id: str
                The id of the study to query.
        """
        api_client = NMDClient()
        dp = DataProcessing()
        url = f"{api_client.base_url}/studies/{study_id}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error: " + response.text)
        results = response.json()
        return results

    def find_study_by_attribute(
        self, attribute_name, attribute_value, page_size=25
    ) -> List[Dict]:
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
        filter = f"{attribute_name}.search:{attribute_value}"

        # Encode the filter for use in the URL
        encoded_filter = urllib.parse.quote(filter)

        url = f"{api_client.base_url}/studies?filter={encoded_filter}&per_page={page_size}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()["results"]
        return results

    def find_study_by_filter(self, filter, page_size=25) -> List[Dict]:
        """
        Get a study from the NMDC API by its name. Studies can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Study/.
        params:
            filter: str
                The filter to use to query the studies.
                Example: id:my_id, name:my_study, description:my_description

            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()

        url = (
            f"{api_client.base_url}/studies?&filter={filter}&max_page_size={page_size}"
        )
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()["results"]
        return results

    def find_study_by_pi(self, pi_name: str, page_size=25) -> List[Dict]:
        """
        Get a study from the NMDC API by its name. Studies can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Study/.
        params:
            pi_name: str
                The name of the PI to filter by.
            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()
        filter = f"principal_investigator.has_raw_value.search:{pi_name}"

        url = f"{api_client.base_url}/studies?filter={filter}&per_page={page_size}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()["results"]
        return results

    def get_study_data_objects(self, study_id: str) -> List[Dict]:
        """
        Get the data objects associated with a study.
        params:
            study_id: str
                The id of the study to query.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/data_objects/studies/{study_id}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()["results"]
        return results


if __name__ == "__main__":
    pass
