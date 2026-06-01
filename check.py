import os, json, sys
sys.path.insert(0, 'src')
from dotenv import load_dotenv; load_dotenv()
from src.eia_client import EIAClient
client = EIAClient(os.getenv('EIA_API_KEY'))
records = client._paginate('/electricity/electric-power-operational-data/data', [
    ('frequency','monthly'),('data[]','generation'),
    ('start','2023-01'),('end','2023-01'),('length','1')
])
print(json.dumps(records[0], indent=2))