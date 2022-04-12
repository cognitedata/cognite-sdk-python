import os
from cognite.client import CogniteClient

CLIENT_ID = "a29a9a25-7294-4675-81f8-a65804552b94"
CLIENT_SECRET = os.environ.get("CDF_CLIENT_SECRET")
CLUSTER = "bluefield"
SCOPES = [f"https://{CLUSTER}.cognitedata.com/.default"]
TENANT_ID = "cognitecharts.onmicrosoft.com"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
DST_PROJECT = "charts-demo"
CLIENT_NAME = "CDF Replicator"


client = CogniteClient(
    token_url=TOKEN_URL,
    token_client_id=CLIENT_ID,
    token_client_secret=CLIENT_SECRET,
    token_scopes=SCOPES,
    project=DST_PROJECT,
    base_url=f"https://{CLUSTER}.cognitedata.com",
    client_name=CLIENT_NAME
)

# items = client.datapoints.retrieve(external_id='pi:160883', start='10d-ago', end='now', input_unit='m', output_unit='ft')

# tests = items

from cognite.client.data_classes import TimeSeries

ts = TimeSeries(external_id='test2_external_id', 
           name='test2_name', 
           description='test2_description', 
           unit='lbf/ft4')

client.time_series.create(ts)