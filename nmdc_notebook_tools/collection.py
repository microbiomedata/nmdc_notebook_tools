# -*- coding: utf-8 -*-
import requests
from nmdc_notebook_tools.data_processing import DataProcessing

from nmdc_notebook_tools.api import NMDClient


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
        url = f"{api_client.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}"
        # get the reponse
        response = requests.get(url)

        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")

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
            response = requests.get(url)
            if response.status_code == 200:
                # combine the previous api call json with the new one
                results = {
                    "resources": results["resources"] + response.json()["resources"]
                }
            else:
                return (response.status_code, "There was an error.")
        return results

    def get_collection_by_type(
        self,
        collection_name: str,
        data_object_type: str = "",
        max_page_size: int = 100,
        fields: str = "",
        all_pages: bool = False,
    ):
        """
        Get a collection of data from the NMDC API. Specific function to get a collection of data from the NMDC API, filtered by data object type.
        params:
            collection_name: str
                The name of the collection to query. Name examples can be found here https://microbiomedata.github.io/nmdc-schema/Database/
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
        filter = '{"data_object_type":{"$regex": "{data_object_type}"}}'.format(
            data_object_type=data_object_type
        )
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        url = f"{api_client.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")

        results = response.json()["resources"]
        # otherwise, get all pages
        if all_pages:
            results = self._get_all_pages(
                response, collection_name, filter, max_page_size, fields
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
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")

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
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code != 200:
            return (response.status_code, "There was an error.")

        collection_name = response.json()["collection_name"]

        return collection_name


if __name__ == "__main__":
    get_collection = Collection()
    get_collection.get_collection("Database")
