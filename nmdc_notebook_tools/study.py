# -*- coding: utf-8 -*-
from nmdc_notebook_tools.api import NMDClient
import requests
import urllib.parse
from typing import List, Dict
from nmdc_notebook_tools.data_processing import DataProcessing
from nmdc_notebook_tools.utils import Utils
import logging

logger = logging.getLogger(__name__)


class Study:
    def __init__(self):
        pass

    def study_by_id(self, study_id: str) -> Dict:
        """
        Get a study from the NMDC API by its id.
        params:
            study_id: str
                The id of the study to query.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/nmdcschema/study_set/{study_id}"
        # get the reponse
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get study by id from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()
        return results

    def study_by_attribute(
        self, attribute_name, attribute_value, page_size=25, fields=""
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
            fields: str
                Specify fields to return in the response in a comma separated list. Default is all fields.
                    Example: "id,name,description"

        """
        api_client = NMDClient()
        filter = f'{{"{attribute_name}": "{attribute_value}"}}'
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/study_set?filter={filter}&per_page={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get study from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def study_by_filter(self, filter, page_size=25, fields="") -> List[Dict]:
        """
        Get a study from the NMDC API by its name. Studies can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Study/.
        params:
            filter: str
                The filter to use to query the studies. Must be in the form of a MongoDB query.
                Example: '{"id":"nmdc:study-0001"}'
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                Specify fields to return in the response in a comma separated list. Default is all fields.
                    Example: "id,name,description"
        """
        api_client = NMDClient()
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/study_set/?&filter={filter}&max_page_size={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get study from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def find_study_by_pi(self, pi_name: str, page_size=25, fields="") -> List[Dict]:
        """
        Get a study from the NMDC API by its name. Studies can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/Study/.
        params:
            pi_name: str
                The name of the PI to filter by.
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                Specify fields to return in the response in a comma separated list. Default is all fields.
                    Example: "id,name,description"
        """
        api_client = NMDClient()
        filter = urllib.parse.quote_plus(
            f'{{"principal_investigator.has_raw_value": {pi_name}}}'
        )
        url = f"{api_client.base_url}/nmdcschema/study_set/?filter={filter}&per_page={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get study by pi from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
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
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get study data objects from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["results"]
        return results


if __name__ == "__main__":
    pass
