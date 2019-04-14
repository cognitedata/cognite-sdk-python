import cProfile
import functools
import gzip
import json

BASE_URL = "https://api.cognitedata.com"


def jsgz_load(s):
    return json.loads(gzip.decompress(s).decode())


def profile(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        method(*args, **kwargs)
        pr.disable()
        pr.print_stats(sort="cumtime")

    return wrapper
