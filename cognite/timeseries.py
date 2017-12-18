import cognite.config as config
import io
import json
import pandas as pd
import requests

def get_datapoints_frame(tagIds, aggregates, granularity, start=None, end=None, limit=10000, api_key=None, project=None):
    api_key, project = config.get_config_variables(api_key, project)
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/dataframe'.format(project)
    body = {
        'items': [{'tagId': '{}'.format(tagId)} for tagId in tagIds],
        'aggregates': aggregates,
        'granularity': granularity,
        'limit': limit
    }
    if start:
        body['start'] = start
    if end:
        body['end'] = end

    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'text/csv'
    }

    r = requests.post(url=url, data=json.dumps(body), headers=headers)
    return pd.read_csv(io.StringIO(r.content.decode('utf-8')))

