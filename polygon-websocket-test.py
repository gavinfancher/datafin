import dotenv
import os

dotenv.load_dotenv()

polygon_api_key = os.getenv('POLYGON_API_KEY')
w

from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

client = WebSocketClient(
	api_key = polygon_api_key,
	feed = Feed.Delayed,
	market = Market.Stocks
	)

# aggregates (per second)
client.subscribe("A.Spy", "AM.AMZN") # multiple tickers

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)