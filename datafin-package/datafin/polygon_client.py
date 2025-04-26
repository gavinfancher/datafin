
from polygon import RESTClient






class PolygonClient:
    def __init__(
            self,
            api_key: str
    ):
        self.api_key = api_key
        self.client = RESTClient(api_key=self.api_key)


    def get_1min_aggs(
            self
    ):
        pass