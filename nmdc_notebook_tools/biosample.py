# -*- coding: utf-8 -*-
from nmdc_notebook_tools.api import NMDClient
import requests
import urllib.parse
from typing import List, Dict
from nmdc_notebook_tools.data_processing import DataProcessing
import logging
from nmdc_notebook_tools.utils import Utils

logger = logging.getLogger(__name__)


class Biosample:
    def __init__(self):
        pass

    def get_all_biosamples(
        self, fields="", all_pages=False, page_size=25
    ) -> List[Dict]:
        """
        Get all biosamples from the NMDC API.
        params:
            fields: str
                The fields to return. Default is all fields.
            all_pages: bool
                Whether or not to return all pages of results. Default is False.
            page_size: int
                The number of results to return per page. Default is 25.
        returns:
            List[Dict]: A list of biosamples.
        Raises:
            RuntimeError: An error is raised if the API request fails.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/nmdcschema/biosample_set?max_page_size={page_size}&projection={fields}"
        # get the reponse
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get biosamples from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        # otherwise, get all pages
        if all_pages:
            ut = Utils()
            results = ut.get_all_pages(
                response, "biosample_set", filter, page_size, fields
            )["resources"]

        results = response.json()["results"]
        return results

    def biosample_by_id(self, sample_id: str) -> Dict:
        """
        Get a biosample from the NMDC API by its id.
        params:
            sample_id: str
                The id of the biosample to query.
        """
        api_client = NMDClient()
        filter = urllib.parse.quote_plus(f'{{"id":"{sample_id}"}}')
        url = f"{api_client.base_url}/nmdcschema/biosample_set/?filter={filter}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get biosample from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def biosample_by_filter(self, filter: str, page_size=25) -> Dict:
        """
        Get a biosample from the NMDC API by its id.
        params:
            filter: str
                The filter to use to query the biosample. Must be in MonogDB query format.
                    Resources found here - https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#std-label-method-find-query
                Example: {"name":{"my biosample name"}}
            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/biosample_set/?filter={filter}&per_page={page_size}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get biosample(s) from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def biosample_by_attribute(
        self, attribute_name, attribute_value, page_size=25
    ) -> List[Dict]:
        """
        Get a biosample from the NMDC API by its name. Biosamples can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Biosample/.
        params:
            attribute_name: str
                The name of the attribute to filter by.
            attribute_value: str
                The value of the attribute to filter by.
            page_size: int
                The number of results to return per page. Default is 25.
        """
        api_client = NMDClient()
        filter = f'{{"{attribute_name}": "{attribute_value}"}}'
        print(filter)
        filter = urllib.parse.quote_plus(filter)

        url = f"{api_client.base_url}/nmdcschema/biosample_set?filter={filter}&per_page={page_size}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get biosample from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results


if __name__ == "__main__":
    pass
