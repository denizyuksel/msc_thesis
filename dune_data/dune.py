from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas as pd
import config

# Initialize the Dune client
dune = DuneClient(
    api_key=config.dune_api_key,
    base_url="https://api.dune.com",
    request_timeout=config.request_timeout
)

""" get the latest executed result without triggering a new execution """
# query_result = dune.get_latest_result(config.query_id) # get latest result in json format
query_result = dune.get_latest_result_dataframe(config.hourly_query_id) # get latest result in Pandas dataframe format

print(f"QUERY RESULT 1: {query_result}")
query_result.to_csv('mevblocker_deniz_18_distinct.csv', index=False)