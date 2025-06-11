from dagster import Definitions


from .jobs.full_market_aggs import polygon_full_market_aggs_definition
from .jobs.spy_open_aggs import polygon_spy_open_minute_definition
from .jobs.spy_close_aggs import polygon_spy_close_minute_definition



polygon_definitions = Definitions.merge(
    polygon_full_market_aggs_definition,
    polygon_spy_open_minute_definition,
    polygon_spy_close_minute_definition
)