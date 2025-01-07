# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection import Collection
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
