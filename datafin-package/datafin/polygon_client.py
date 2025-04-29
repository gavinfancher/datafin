from polygon import RESTClient



class PolygonClient:
    def __init__(
            self,
            api_key: str
    ):
        self.api_key = api_key
        self.client = RESTClient(api_key=self.api_key)


    def get_eod_aggs(
            self,
            date: str,
            symbol: str
    ):
        response = self.client.get_daily_open_close_agg(
            ticker=symbol,
            date=date,
            adjusted="true",
        )

        return response
    

    def get_eod_second_aggs(
            self,
            symbol: str,
            date: str
    ):

        aggs = []
        for a in self.client.list_aggs(
            symbol,
            1,
            "second",
            date,
            date,
            adjusted="true",
            sort="asc",
        ):
            aggs.append(a)

        return aggs
