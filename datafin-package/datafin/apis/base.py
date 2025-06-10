from typing import Any, Dict, Optional, Union

import requests
from requests.exceptions import RequestException


class APICall:
    def __init__(
        self,
        base_url: str = None,
        api_key: str = None
    ) -> None:
        
        """
        docs
        """

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

        if self.api_key is None or self.base_url is None:
            raise ValueError('api_key and base_url must be provided')
        

    #######################################################
    #######################################################


    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Union[Dict[str, Any], str]:
        """
        Make a GET request to the specified endpoint
        
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = params or {}
        headers = headers or {}


        url = f'{self.base_url}/{endpoint}'
        response = requests.get(url, params=params)

        if response.status_code != 200:
            return response.raise_for_status()
        else:
            data = response.json()
            if not data:
                return "empty response"
            else:
                return data