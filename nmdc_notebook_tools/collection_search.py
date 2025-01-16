# -*- coding: utf-8 -*-
import requests
from nmdc_notebook_tools.data_processing import DataProcessing
import urllib.parse
from nmdc_notebook_tools.nmdc_search import NMDCSearch
import logging

logger = logging.getLogger(__name__)


class CollectionSearch:
    """
    Class to interact with the NMDC API to get collections of data. Must know the collection name to query.
    """

    def __init__(self, collection_name):
        self.collection_name = collection_name

    def get_record(
        self,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
        all_pages: bool = False,
    ):
        """
        Get a collection of data from the NMDC API. Generic function to get a collection of data from the NMDC API. Can provide a specific filter if desired.
        params:
            filter: str
                The filter to apply to the query. Default is an empty string.
            max_page_size: int
                The maximum number of items to return per page. Default is 100.
            fields: str
                The fields to return. Default is all fields.
        """
        api_client = NMDCSearch()
        dp = DataProcessing()
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}"
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
                response, self.collection_name, filter, max_page_size, fields
            )["resources"]

        return dp.convert_to_df(results)

    def _get_all_pages(
        self,
        response: requests.models.Response,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
    ):
        results = response.json()
        api_client = NMDCSearch()
        while True:
            if response.json().get("next_page_token"):
                next_page_token = response.json()["next_page_token"]
            else:
                break
            url = f"{api_client.base_url}/nmdcschema/{self.collection_name}?filter={filter}&page_size={max_page_size}&projection={fields}&page_token={next_page_token}"
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

    def get_record_by_filter(self, filter: str, page_size=25, fields=""):
        """
        Get a record from the NMDC API by its id.
        params:
            filter: str
                The filter to use to query the collection. Must be in MonogDB query format.
                    Resources found here - https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#std-label-method-find-query
                Example: {"name":{"my record name"}}
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                The fields to return. Default is all fields.
                Example: "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        """
        api_client = NMDCSearch()
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}/?filter={filter}&max_page_size={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError(
                f"Failed to get {self.collection_name}(s) from NMDC API"
            ) from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def get_record_by_attribute(
        self, attribute_name, attribute_value, page_size=25, fields=""
    ):
        """
        Get a record from the NMDC API by its name. Records can be filtered based on their attributes found https://microbiomedata.github.io/nmdc-schema/.
        params:
            attribute_name: str
                The name of the attribute to filter by.
            attribute_value: str
                The value of the attribute to filter by.
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                The fields to return. Default is all fields.
        """
        api_client = NMDCSearch()
        filter = f'{{"{attribute_name}":{{"$regex":"{attribute_value}"}}}}'
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}?filter={filter}&max_page_size={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError(
                f"Failed to get {self.collection_name} from NMDC API"
            ) from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def get_record_by_latitude(
        self, comparison: str, latitude: float, page_size=25, fields=""
    ):
        """
        Get a record from the NMDC API by latitude comparison.
        params:
            comparison: str
                The comparison to use to query the record. MUST BE ONE OF THE FOLLOWING:
                    eq    - Matches values that are equal to the given value.
                    gt    - Matches if values are greater than the given value.
                    lt    - Matches if values are less than the given value.
                    gte    - Matches if values are greater or equal to the given value.
                    lte - Matches if values are less or equal to the given value.
            latitude: float
                The latitude of the record to query.
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                The fields to return. Default is all fields.
                Example: "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        """
        allowed_comparisons = ["eq", "gt", "lt", "gte", "lte"]
        if comparison not in allowed_comparisons:
            logger.error(
                f"Invalid comparison input: {comparison}\n Valid inputs: {allowed_comparisons}"
            )
            raise ValueError(
                f"Invalid comparison input: {comparison}\n Valid inputs: {allowed_comparisons}"
            )
        api_client = NMDCSearch()
        filter = f'{{"lat_lon.latitude": {{"${comparison}": {latitude}}}}}'
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}/?filter={filter}&max_page_size={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError(
                f"Failed to get {self.collection_name} from NMDC API"
            ) from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        return response.json()["resources"]

    def get_record_by_longitude(
        self, comparison: str, longitude: float, page_size=25, fields=""
    ):
        """
        Get a record from the NMDC API by longitude comparison.
        params:
            comparison: str
                The comparison to use to query the record. MUST BE ONE OF THE FOLLOWING:
                    eq    - Matches values that are equal to the given value.
                    gt    - Matches if values are greater than the given value.
                    lt    - Matches if values are less than the given value.
                    gte    - Matches if values are greater or equal to the given value.
                    lte - Matches if values are less or equal to the given value.
            longitude: float
                The longitude of the record to query.
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                The fields to return. Default is all fields.
                Example: "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        """
        allowed_comparisons = ["eq", "gt", "lt", "gte", "lte"]
        if comparison not in allowed_comparisons:
            logger.error(
                f"Invalid comparison input: {comparison}\n Valid inputs: {allowed_comparisons}"
            )
            raise ValueError(
                f"Invalid comparison input: {comparison}\n Valid inputs: {allowed_comparisons}"
            )
        api_client = NMDCSearch()
        filter = f'{{"lat_lon.longitude": {{"${comparison}": {longitude}}}}}'
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}/?filter={filter}&max_page_size={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError(
                f"Failed to get {self.collection_name} from NMDC API"
            ) from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        return response.json()["resources"]

    def get_record_by_lat_long(
        self,
        lat_comparison: str,
        long_comparison: str,
        latitude: float,
        longitude: float,
        page_size=25,
        fields="",
    ):
        """
        Get a record from the NMDC API by latitude and longitude comparison.
        params:
            lat_comparison: str
                The comparison to use to query the record for latitude. MUST BE ONE OF THE FOLLOWING:
                    eq    - Matches values that are equal to the given value.
                    gt    - Matches if values are greater than the given value.
                    lt    - Matches if values are less than the given value.
                    gte    - Matches if values are greater or equal to the given value.
                    lte - Matches if values are less or equal to the given value.
            long_comparison: str
                The comparison to use to query the record for longitude. MUST BE ONE OF THE FOLLOWING:
                    eq    - Matches values that are equal to the given value.
                    gt    - Matches if values are greater than the given value.
                    lt    - Matches if values are less than the given value.
                    gte    - Matches if values are greater or equal to the given value.
                    lte - Matches if values are less or equal to the given value.
            latitude: float
                The latitude of the record to query.
            longitude: float
                The longitude of the record to query.
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                The fields to return. Default is all fields.
                Example: "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        """
        allowed_comparisons = ["eq", "gt", "lt", "gte", "lte"]
        if lat_comparison not in allowed_comparisons:
            logger.error(
                f"Invalid comparison input: {lat_comparison}\n Valid inputs: {allowed_comparisons}"
            )
            raise ValueError(
                f"Invalid comparison input: {lat_comparison}\n Valid inputs: {allowed_comparisons}"
            )
        if long_comparison not in allowed_comparisons:
            logger.error(
                f"Invalid comparison input: {long_comparison}\n Valid inputs: {allowed_comparisons}"
            )
            raise ValueError(
                f"Invalid comparison input: {long_comparison}\n Valid inputs: {allowed_comparisons}"
            )
        api_client = NMDCSearch()
        filter = f'{{"lat_lon.latitude": {{"${lat_comparison}": {latitude}}}, "lat_lon.longitude": {{"${long_comparison}": {longitude}}}}}'
        filter = urllib.parse.quote_plus(filter)
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}/?filter={filter}&per_page={page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError(
                f"Failed to get {self.collection_name} from NMDC API"
            ) from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )
        results = response.json()["resources"]
        return results

    def get_record_data_object_by_type(
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
        api_client = NMDCSearch()
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

    def get_record_by_id(
        self,
        collection_id: str,
        max_page_size: int = 100,
        fields: str = "",
    ):
        """
        Get a collection of data from the NMDC API by id.
        params:
            collection_id: str
                The id of the collection.
            max_page_size: int
                The maximum number of items to return per page. Default is 100.
            fields: str
                The fields to return. Default is all fields.
        """
        results = []
        api_client = NMDCSearch()
        dp = DataProcessing()
        # if fields is empty, return all fields
        if not fields:
            fields = "id,name,description,alternative_identifiers,file_size_bytes,md5_checksum,data_object_type,url,type"
        url = f"{api_client.base_url}/nmdcschema/{self.collection_name}/{collection_id}?page_size={max_page_size}&projection={fields}"
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

    def get_record_name_from_id(self, doc_id: str):
        """
        Used when you have an id but not the collection name.
        Determine the schema class by which the id belongs to.
        Sets the collection_name attribute.
        params:
            doc_id: str
                The id of the document.
        """
        api_client = NMDCSearch()
        url = f"{api_client.base_url}/nmdcschema/ids/{doc_id}/collection-name"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", exc_info=True)
            raise RuntimeError("Failed to get record from NMDC API") from e
        else:
            logging.debug(
                f"API request response: {response.json()}\n API Status Code: {response.status_code}"
            )

        collection_name = response.json()["collection_name"]
        self.collection_name = collection_name
        return collection_name


if __name__ == "__main__":
    pass
