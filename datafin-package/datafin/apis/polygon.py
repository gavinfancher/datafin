from polygon import RESTClient

from typing import Optional

import datetime

class PolygonClient:
    def __init__(
            self,
            api_key: str
    ):
        self.api_key = api_key
        self.client = RESTClient(api_key=self.api_key)
    


    def get_aggs(
            self,
            symbol: str,
            multiplier: int,
            unit: str,
            _from: datetime,
            _to: datetime
    ):
        allowed_units = [
            'second',
            'minute',
            'hour',
            'day',
            'week',
            'month',
            'quarter',
            'year'
        ]
        assert unit in allowed_units

        aggs = []
        for a in self.client.list_aggs(
            ticker=symbol.upper(),
            multiplier=multiplier,
            timespan=unit,
            from_=_from,
            to=_to
        ):
            aggs.append(a)

        return aggs
