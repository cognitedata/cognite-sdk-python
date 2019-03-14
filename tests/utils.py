import gzip
import json

BASE_URL = "https://api.cognitedata.com"


def get_call_args_data_from_mock(mock, index, decompress_gzip=False):
    data = mock.call_args_list[index][1]["data"]
    if decompress_gzip:
        data = gzip.decompress(data).decode()
    return json.loads(data)
