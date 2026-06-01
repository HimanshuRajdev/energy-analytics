import os
from src.eia_client import EIAClient

from dotenv import load_dotenv

load_dotenv()  # reads .env and populates os.environ

client = EIAClient(api_key=os.environ["EIA_API_KEY"])
prices = client.get_retail_prices(start="2020-01", end="2020-03")

print(f"Total records: {len(prices)}")
print(f"First record: {prices[0]}")
print(f"Unique states: {len(set(r['stateid'] for r in prices))}")
print(f"Sectors: {set(r['sectorName'] for r in prices)}")
