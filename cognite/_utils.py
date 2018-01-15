# -*- coding: utf-8 -*-
"""Utilites for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not used by end-users.
"""
import json
import math
import re
import sys
import requests
import cognite._constants as _constants

def get_request(url, params=None, headers=None):
    '''Perform a GET request with a predetermined number of retries.'''
    for _ in range(_constants.RETRY_LIMIT + 1):
        res = requests.get(url, params=params, headers=headers)
        if res.status_code == 200:
            return res
    raise _APIError(res.json()['error'])

def post_request(url, body, headers=None):
    '''Perform a POST request with a predetermined number of retries.'''
    for _ in range(_constants.RETRY_LIMIT + 1):
        res = requests.post(url, data=json.dumps(body), headers=headers)
        if res.status_code == 200:
            return res
    raise _APIError(res.json()['error'])

def granularity_to_ms(time_string):
    '''Returns millisecond representation of granularity time string'''
    magnitude = int(''.join([c for c in time_string if c.isdigit()]))
    unit = ''.join([c for c in time_string if c.isalpha()])
    unit_in_ms = {'s': 1000, 'second': 1000, 'm': 60000, 'minute': 60000,
                  'h': 3600000, 'hour': 3600000, 'd': 86400000, 'day': 86400000}
    return magnitude * unit_in_ms[unit]

def _time_ago_to_ms(time_ago_string):
    '''Returns millisecond representation of time-ago string'''
    pattern = r'(\d+)([a-z])-ago'
    res = re.match(pattern, str(time_ago_string))
    if res:
        magnitude = int(res.group(1))
        unit = res.group(2)
        unit_in_ms = {'s': 1000, 'm': 60000, 'h': 3600000, 'd': 86400000, 'w': 604800000}
        return magnitude * unit_in_ms[unit]
    return None

class _APIError(Exception):
    pass

class ProgressIndicator():
    '''This class lets the system give the user and indication of how much data remains to be downloaded in the request'''
    def __init__(self, tag_ids, start, end):
        self.start, self.end = self._get_start_end(tag_ids, start, end)
        self.length = self.end - self.start
        self.progress = 0.0
        sys.stdout.write("\rDownloading requested data: {:5.1f}% |{}|".format(0, ' ' * 20))
        sys.stdout.flush()
        self._print_progress()

    def update_progress(self, latest_timestamp):
        '''Update progress based on latest timestamp'''
        self.progress = 100 - (((self.end - latest_timestamp) / self.length) * 100)
        self._print_progress()

    def _print_progress(self):
        prog = int(math.ceil(self.progress) / 5)
        remainder = 20 - prog
        sys.stdout.write("\rDownloading requested data: {:5.1f}% |{}|".format(int(math.ceil(self.progress)), '|' * prog + ' ' * remainder))
        sys.stdout.flush()

    def terminate(self):
        sys.stdout.write("\rDownloading requested data: {:5.1f}% |{}|".format(100, '|' * 20))
        sys.stdout.flush()
        sys.stdout.write("\r" + " " * 500)
        sys.stdout.flush()

    def _get_start_end(self, tag_ids, start, end):
        from cognite.timeseries import get_latest, get_datapoints
        first_timestamp = float("inf")
        for tag in tag_ids:
            if isinstance(tag, str):
                datapoints = get_datapoints(tag, limit=1, start=start, end=end).to_json()['datapoints']
            else:
                datapoints = get_datapoints(tag['tagId'], limit=1).to_json()['datapoints']
            tag_start = float("inf")
            if datapoints:
                tag_start = int(datapoints[0]['timestamp'])
            if tag_start < first_timestamp:
                first_timestamp = tag_start
        latest_timestamp = 0

        for tag in tag_ids:
            if isinstance(tag, str):
                tag_end = int(get_latest(tag).to_json()['timestamp'])
            else:
                tag_end = int(get_latest(tag['tagId']).to_json()['timestamp'])
            if tag_end > latest_timestamp:
                latest_timestamp = tag_end

        if start is None:
            start = first_timestamp
        elif _time_ago_to_ms(start): # start is specified as string of format '1d-ago'
            start = latest_timestamp - _time_ago_to_ms(start)
        else:
            start = start

        if end is None:
            end = latest_timestamp
        elif _time_ago_to_ms(end): # end is specified as string of format '1d-ago'
            end = latest_timestamp - _time_ago_to_ms(end)
        else:
            end = end

        return start, end
