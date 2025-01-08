# -*- coding: utf-8 -*-
import requests
from nmdc_notebook_tools.data_processing import DataProcessing
import urllib.parse
from nmdc_notebook_tools.api import NMDClient
import logging

logger = logging.getLogger(__name__)


class Collection:
    def __init__(self):
        pass

    def get_collection(
        self,
        collection_name: str,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
        all_pages: bool = False,
    ):
        """
        Get a collection of data from the NMDC API. Generic function to get a collection of data from the NMDC API. Can provide a specific filter if desired.
        params:
            collection_name: str
                The name of the collection to query. Name examples can be found here https://microbiomedata.github.io/nmdc-schema/Database/
            filter: str
                The filter to apply to the query. Default is an empty string.
            max_page_size: int
                The maximum number of items to return per page. Default is 100.
            fields: str
                The fields to return. Default is all fields.
        """
        api_client = NMDClient()
        dp = DataProcessing()
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get collection from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )

        results = response.json()["resources"]
        # otherwise, get all pages
        if all_pages:
            results = self._get_all_pages(
                response, collection_name, filter, max_page_size, fields
            )["resources"]

        return dp.convert_to_df(results)

    def _get_all_pages(
        self,
        response: requests.models.Response,
        collection_name: str,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
    ):
        results = response.json()
        api_client = NMDClient()
        while True:
            if response.json().get("next_page_token"):
                next_page_token = response.json()["next_page_token"]
            else:
                break
            url = f"{api_client.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}&page_token={next_page_token}"
            try:
                response = requests.get(url)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error("API request failed", exc_info=True)
                raise RuntimeError("Failed to get collection from NMDC API") from e
            else:
                logging.debug(
                    f"API request response: {response.json()}\n API Status Code: {response.status_code}"
                )
            results = {"resources": results["resources"] + response.json()["resources"]}
        return results

    def get_collection_data_object_by_type(
        self,
        data_object_type: str = "",
        max_page_size: int = 100,
        fields: str = "",
        all_pages: bool = False,
    ):
        """
        Get a collection of data from the NMDC API. Specific function to get a collection of data from the NMDC API, filtered by data object type.
        params:
            data_object_type: str
                The data_object_type to filter by. Default is an empty string, which will return all data.
            max_page_size: int
                The maximum number of items to return per page. Default is 100.
            fields: str
                The fields to return. Default is all fields.
            pages: bool
                True to return all pages. False to return the first page. Default is False.
        """
        results = []
        api_client = NMDClient()
        dp = DataProcessing()
        # create the filter based on data object type
        filter = f'{{"data_object_type":{{"$regex": "{data_object_type}"}}}}'
        filter = urllib.parse.quote_plus(filter)
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        url = f"{api_client.base_url}/nmdcschema/data_object_set?filter={filter}&page_size={max_page_size}&projection={fields}"
        # get the reponse
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get data object from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        # otherwise, get all pages
        if all_pages:
            results = self._get_all_pages(
                response, "data_object_set", filter, max_page_size, fields
            )["resources"]
        return dp.convert_to_df(results)

    def get_collection_by_id(
        self,
        collection_name: str,
        collection_id: str,
        max_page_size: int = 100,
        fields: str = "",
    ):
        """
        Get a collection of data from the NMDC API by id.
        params:
            collection_name: str
                The name of the collection to query. Name examples can be found here https://microbiomedata.github.io/nmdc-schema/Database/
            collection_id: str
                The id of the collection.
            max_page_size: int
                The maximum number of items to return per page. Default is 100.
            fields: str
                The fields to return. Default is all fields.
        """
        results = []
        api_client = NMDClient()
        dp = DataProcessing()
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        url = f"{api_client.base_url}/nmdcschema/{collection_name}/{collection_id}?page_size={max_page_size}&projection={fields}"
        # get the reponse
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get collection by id from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )

        results = response.json()["resources"]

        return dp.convert_to_df(results)

    def get_collection_name_from_id(self, doc_id: str):
        """
        Determine the schema class by which the id belongs to.
        params:
            doc_id: str
                The id of the document.
        """
        api_client = NMDClient()
        url = f"{api_client.base_url}/nmdcschema/ids/{doc_id}/collection-name"
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

        collection_name = response.json()["collection_name"]

        return collection_name


if __name__ == "__main__":
    pass
