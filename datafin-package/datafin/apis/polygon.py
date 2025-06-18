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
            _to: datetime,
            limit: int = 5000
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
        assert unit in allowed_units, f"Invalid unit '{unit}'. Must be one of: {', '.join(allowed_units)}"

        aggs = []
        for a in self.client.list_aggs(
            ticker=symbol.upper(),
            multiplier=multiplier,
            timespan=unit,
            from_=_from,
            to=_to,
            limit=limit
        ):
            aggs.append(a)

        return aggs
    

    def get_previous_close_agg(
            self,
            symbol
    ):
        aggs = self.client.get_previous_close_agg(
            ticker=symbol.upper(),
            adjusted="true",
        )
        return aggs
