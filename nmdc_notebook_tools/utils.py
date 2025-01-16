# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection import Collection
import requests
from nmdc_notebook_tools.api import NMDClient
import logging

logger = logging.getLogger(__name__)


class Utils:
    def __init__(self):
        pass

    def string_mongo_list(self, data: list) -> str:
        """
        Convert elements in a list to use double quotes instead of single quotes.
        This is required for mongo queries.
        """
        return str(data).replace("'", '"')

    def split_list(self, data: list, chunk_size: int) -> list:
        return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

    def get_id_list(self, data: list, id_name: str) -> list:
        """
        Get a list of ids from an api call response json.

        """
        return [item[id_name] for item in data]

    def get_id_results(
        self,
        newest_results: list,
        id_field: str,
        query_collection: str,
        match_id_field: str,
        query_fields: str,
    ) -> list:
        """
        Get the results from a query collection based on the ids from the newest results.
        params:
            newest_results: list
                The results from the most recent query.
            id_field: str
                The field in the newest results that contains the ids.
            query_collection: str
                The collection to query.
            match_id_field: str
                The field in the query collection that matches the id_field.
            query_fields: str
                The fields to return in the query.
        """
        # Extract IDs and split them into chunks
        result_ids = self.get_id_list(newest_results, id_field)
        chunked_list = self.split_list(result_ids)

        # Function to construct the appropriate filter string
        def construct_filter_string(chunk):
            filter_string = self.string_mongo_list(chunk)
            if "data_object_type" in match_id_field:
                return f'{{{match_id_field}: {{"$in": {filter_string}}}}}'
            else:
                return f'{{"{match_id_field}": {{"$in": {filter_string}}}}}'

        # Retrieve and collect results
        next_results = []
        for chunk in chunked_list:
            filter_str = construct_filter_string(chunk)
            data = Collection.get_collection(
                query_collection, filter_str, 100, query_fields
            )
            next_results.extend(data["resources"])

        return next_results

    def get_all_pages(
        self,
        response: requests.models.Response,
        collection_name: str,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
    ):
        """
        Get all pages of results from an API request.
        params:
            response: requests.models.Response
                The response object from an API request.
            collection_name: str
                The name of the collection to get results from.
            filter: str
                The filter to apply to the request. Default is an empty string.
            max_page_size: int
                The maximum number of results to return per page. Default is 100.
            fields: str
                The fields to return in the response. Default is an empty string.
        """
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

    def build_filter(self, attributes):
        """
        Create a MongoDB filter using $regex for each attribute in the input dictionary. For nested attributes, use dot notation.

        Parameters:
        attributes (dict): Dictionary of attribute names and their corresponding values to match using regex.
            Example: {"name": "example", "description": "example", "geo_loc_name": "example"}
        Returns:
        dict: A MongoDB filter dictionary.
        """
        filter_dict = {}
        for attribute_name, attribute_value in attributes.items():
            filter_dict[attribute_name] = {"$regex": attribute_value}

        print(self.string_mongo_list(filter_dict))
        return self.string_mongo_list(filter_dict)
