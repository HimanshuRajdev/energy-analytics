# src/eia_client.py
import requests
import time
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class EIAClient:
    """
    Wrapper for the EIA Open Data API v2.
    Handles authentication, pagination, and rate limiting.
    """
    
    BASE_URL = "https://api.eia.gov/v2"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()  # reuses the TCP connection across calls
    
    def _get(self, endpoint: str, params: list) -> Dict:
        """
        Makes a single GET request. Raises an exception if the
        response status is not 200 so the caller always gets clean data
        or a clear error — never silent garbage.
        params must be a list of tuples so repeated keys (e.g. data[])
        are preserved correctly by requests.
        """
        full_params = list(params) + [("api_key", self.api_key)]
        response = self.session.get(f"{self.BASE_URL}{endpoint}", params=full_params)
        response.raise_for_status()  # raises HTTPError on 4xx/5xx
        return response.json()
    
    def _paginate(self, endpoint: str, params: list) -> List[Dict]:
        all_records = []
        offset = 0
        page_size = 5000

        while True:
            paginated_params = list(params) + [
                ("offset", offset),
                ("length", page_size),
            ]

            data = self._get(endpoint, paginated_params)
            records = data["response"]["data"]
            total = int(data["response"]["total"])

            all_records.extend(records)

            if len(all_records) >= total:
                break

            offset += page_size
            time.sleep(0.1)

        return all_records
    
    def get_retail_prices(self, start: str, end: str) -> List[Dict]:
        params = [
            ("frequency", "monthly"),
            ("data[]", "price"),       # each field is its own tuple
            ("data[]", "sales"),       # same key repeated three times
            ("data[]", "customers"),   # requests handles this correctly
            ("start", start),
            ("end", end),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "asc"),
        ]
        return self._paginate("/electricity/retail-sales/data", params)
    
    def get_generation_by_fuel(self, start: str, end: str) -> List[Dict]:
        params = [
            ("frequency", "monthly"),
            ("data[]", "generation"),
            ("start", start),
            ("end", end),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "asc"),
        ]
        return self._paginate("/electricity/electric-power-operational-data/data", params)

    def get_rto_demand(self, start: str, end: str) -> List[Dict]:
        """
        Hourly RTO/balancing authority demand from /v2/electricity/rto/region-data.
        Returns demand values (MWh) per region per hour.
        """
        params = [
            ("frequency", "hourly"),
            ("data[]", "value"),
            ("start", start),
            ("end", end),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "asc"),
        ]
        return self._paginate("/electricity/rto/region-data/data", params)