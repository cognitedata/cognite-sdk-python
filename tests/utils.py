import gzip
import json

BASE_URL = "https://api.cognitedata.com"


def jgz_load(str):
    return json.loads(gzip.decompress(str))
