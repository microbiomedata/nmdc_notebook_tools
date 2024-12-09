import requests


class NMDClient:
    def __init__(self):
        self.base_url = "https://api.microbiomedata.org/nmdc_schema/v1"

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}" if self.api_key else None,
        }

    def get_collection(
        self,
        collection_name: str,
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
        pages: int = 1,
    ):
        results = []
        for page in range(1, pages + 1):
            url = f"{self.base_url}/{collection_name}?filter={filter}&page_size={max_page_size}&fields={fields}&page={page}"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                results.extend(response.json()["data"])
            else:
                response.raise_for_status()
        return results

    def get_study_id(self, study_name: str):
        # Example function: Implement the actual endpoint call
        pass
