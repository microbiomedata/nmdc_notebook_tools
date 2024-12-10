# -*- coding: utf-8 -*-
import requests
from src.nmdc_notebook_tools.data_processing import convert_to_df
from src.nmdc_notebook_tools.utils import get_id_list, split_list, string_mongo_list


class NMDClient:
    def __init__(self):
        self.base_url = "https://api.microbiomedata.org/nmdc_schema"

    def get_collection(
        self,
        collection_name: str,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
    ):
        """
        Get a collection of data from the NMDC API.
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
        results = []
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        url = f"{self.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}"
        # get the reponse
        response = requests.get(url)
        # check it came back with OK
        if response.status_code == 200:
            results = response.json()["resources"]
            # return the dataframe
            return convert_to_df(results)
        else:
            return (response.status_code, "There was an error.")

    def get_collection_by_type(
        self,
        collection_name: str,
        data_object_type: str = "",
        max_page_size: int = 100,
        fields: str = "",
        all_pages: bool = False,
    ):
        """
        Get a collection of data from the NMDC API.
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
        # create the filter based on data object type
        filter = '{"data_object_type":{"$regex": "{data_object_type}"}}'
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        url = f"{self.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}"
        # get the reponse
        response = requests.get(url)
        # if all_pages is False, return the first page
        if not all_pages:
            # check it came back with OK
            if response.status_code == 200:
                results = response.json()["resources"]
                # return the dataframe
                return convert_to_df(results)
        else:
            if response.get("next_page_token"):
                next_page_token = response["next_page_token"]
            while True:
                url = f"{self.base_url}/nmdcschema/{collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}&page_token={next_page_token}"
                response = requests.get(url)
                if response.status_code == 200:
                    results.extend(response.json())
                    next_page_token = response.get("next_page_token")
                else:
                    break
        return results

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
        result_ids = get_id_list(newest_results, id_field)
        chunked_list = split_list(result_ids)

        # Function to construct the appropriate filter string
        def construct_filter_string(chunk):
            filter_string = string_mongo_list(chunk)
            if "data_object_type" in match_id_field:
                return f'{{{match_id_field}: {{"$in": {filter_string}}}}}'
            else:
                return f'{{"{match_id_field}": {{"$in": {filter_string}}}}}'

        # Retrieve and collect results
        next_results = []
        for chunk in chunked_list:
            filter_str = construct_filter_string(chunk)
            data = self.get_collection(query_collection, filter_str, 100, query_fields)
            next_results.extend(data["resources"])

        return next_results

    def get_study_id(self, study_name: str):
        # Example function: Implement the actual endpoint call
        pass
