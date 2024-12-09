import requests
from src.nmdc_notebook_tools.data_processing import convert_to_df


class NMDClient:
    def __init__(self):
        self.base_url = "https://api.microbiomedata.org/nmdc_schema"

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

    def get_study_id(self, study_name: str):
        # Example function: Implement the actual endpoint call
        pass
