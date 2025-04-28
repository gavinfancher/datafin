"""
Financial Modeling Prep API client
"""

from typing import Dict, List, Optional, Union, Any
from .utils.api_call import APICall

class FMPClient:
    def __init__(
        self,
        api_key: str,
    ) -> None:
        
        """
        docs
        """

        self.api_key = api_key
        self.base_url = 'https://financialmodelingprep.com/stable/'

        self.api = APICall(
            base_url=self.base_url,
            api_key=self.api_key
        )


    #######################################################
    #######################################################


    def get_quote(
        self,
        symbol: str
    ) -> Dict[str, Any]:
        """
        docs
        """
        endpoint = 'quote'
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }

        response = self.api.get(
            endpoint,
            params=params
        )

        return response


    #######################################################


    def get_forex_eod_5min(
        self,
        symbol: str,
        _from: str,
        _to: str
    ) -> Dict[str, Any]:
        """
        docs
        """
        endpoint = 'historical-chart/5min'

        params = {
            'symbol': symbol,
            'from': _from,
            'to': _to,
            'apikey': self.api_key
        }

        response = self.api.get(
            endpoint,
            params=params
        )

        return response










    def test():
        return 'test'