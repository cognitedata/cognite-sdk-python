import cognite._constants as _constants
import json
import math
import re
import requests
import sys

def _get_request(url, params=None, headers=None):
    for i in range(_constants._RETRY_LIMIT + 1):
        res = requests.get(url, params=params, headers=headers)
        if res.status_code == 200:
            return res
    raise APIError(res.json()['error'])

def _post_request(url, body, headers=None):
    for i in range(_constants._RETRY_LIMIT + 1):
        res = requests.post(url, data=json.dumps(body), headers=headers)
        if res.status_code == 200:
            return res
    raise APIError(res.json()['error'])

def _granularity_to_ms(time_string):
    magnitude = int(''.join([c for c in time_string if c.isdigit()]))
    unit = ''.join([c for c in time_string if c.isalpha()])
    unit_in_ms = {'s': 1000, 'second': 1000, 'm': 60000, 'minute': 60000, 'h': 3600000, 'hour': 3600000, 'd': 86400000, 'day': 86400000}
    return magnitude * unit_in_ms[unit]

def _time_ago_to_ms(time_ago_string):
    pattern = '(\d+)([a-z])-ago'
    res = re.match(pattern, str(time_ago_string))
    if res:
        magnitude = int(res.group(1))
        unit = res.group(2)
        unit_in_ms = {'s': 1000, 'm': 60000, 'h': 3600000, 'd': 86400000, 'w': 604800000}
        return magnitude * unit_in_ms[unit]
    return None

class APIError(Exception):
    pass

class _ProgressIndicator():
    def __init__(self, tagIds, start, end):
        from cognite.timeseries import get_latest, get_datapoints
        
        first_timestamp = float("inf")
        for tag in tagIds:
            if type(tag) == str:
                tag_start = int(get_datapoints(tag, limit=1, start=start, end=end).to_json()['datapoints'][0]['timestamp'])
            else:
                tag_start = int(get_datapoints(tag['tagId'], limit=1).to_json()['datapoints'][0]['timestamp'])
            if tag_start < first_timestamp:
                first_timestamp = tag_start
        latest_timestamp = 0

        for tag in tagIds:
            if type(tag) == str:
                tag_end = int(get_latest(tag).to_json()['timestamp'])
            else:
                tag_end = int(get_latest(tag['tagId']).to_json()['timestamp'])
            if tag_end > latest_timestamp:
                latest_timestamp = tag_end

        if start == None:
            self.start = first_timestamp
        elif _time_ago_to_ms(start): # start is specified as string of format '1d-ago'
            self.start = latest_timestamp - _time_ago_to_ms(start)
        else:
            self.start = start

        if end == None:
            self.end = latest_timestamp
        elif _time_ago_to_ms(end): # end is specified as string of format '1d-ago'
            self.end = latest_timestamp - _time_ago_to_ms(end)
        else:
            self.end = end

        self.length = self.end - self.start
        self.progress = 0.0
        print("Downloading requested data...")
        self._print_progress()

    def _update_progress(self, latest_timestamp):
        self.progress = 100 - (((self.end - latest_timestamp) / self.length) * 100)
        self._print_progress()

    def _print_progress(self):
        prog = int(math.ceil(self.progress) / 5)
        remainder = 20 - prog
        sys.stdout.write("\r{:5.1f}% |{}|".format(self.progress, '|' * prog + ' ' * remainder))
        sys.stdout.flush()
        if int(math.ceil(self.progress)) == 100:
            print()
