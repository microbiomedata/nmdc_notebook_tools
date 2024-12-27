# -*- coding: utf-8 -*-
from nmdc_notebook_tools.api import NMDClient
import requests
import urllib.parse
from typing import List, Dict
from nmdc_notebook_tools.data_processing import DataProcessing


class Biosample:
    def __init__(self):
        pass

    def get_all_biosamples(self, page_size=25) -> List[Dict]:
        """
        TODO
        Get all biosamples from the NMDC API.
        params:
            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/biosamples?per_page={page_size}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error: " + response.text)
        results = response.json()["results"]
        return results

    def find_biosample_by_id(self, sample_id: str) -> Dict:
        """
        Get a biosample from the NMDC API by its id.
        params:
            sample_id: str
                The id of the biosample to query.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/biosamples/{sample_id}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error: " + response.text)
        results = response.json()
        return results

    def find_biosample_by_filter(self, filter: str, page_size=25) -> Dict:
        """
        Get a biosample from the NMDC API by its id.
        params:
            sample_id: str
                The id of the biosample to query.
            filter: str
                The filter to use to query the biosample.
                Example: id:my_id, name:my_sample, description:my_description
            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/biosamples?filter={filter}&per_page={page_size}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error: " + response.text)
        results = response.json()
        return results

    def find_biosample_by_attribute(
        self, attribute_name, attribute_value, page_size=25
    ) -> List[Dict]:
        """
        Get a biosample from the NMDC API by its name. Biosamples can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Biosample/.
        params:
            sample_name: str
                The name of the biosample to query.
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

        url = f"{api_client.base_url}/biosamples?filter={encoded_filter}&per_page={page_size}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")
        results = response.json()["results"]
        return results


if __name__ == "__main__":
    pass
