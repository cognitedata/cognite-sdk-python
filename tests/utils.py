import gzip
import json

BASE_URL = "https://api.cognitedata.com"


def jsgz_load(s):
    return json.loads(gzip.decompress(s).decode())
