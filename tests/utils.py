import gzip
import json

BASE_URL = "https://api.cognitedata.com"


def jsgz_load(str):
    return json.loads(gzip.decompress(str))
