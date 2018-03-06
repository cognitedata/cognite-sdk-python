# -*- coding: utf-8 -*-
"""Utilites for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not used by end-users.
"""
import json
import math
import re
import sys

import gzip

import requests

import cognite._constants as _constants
import cognite.config as config

def get_request(url, params=None, headers=None):
    '''Perform a GET request with a predetermined number of retries.'''
    for _ in range(_constants.RETRY_LIMIT + 1):
        try:
            res = requests.get(url, params=params, headers=headers)
        except Exception as e:
            raise _APIError(e)
        if res.status_code == 200:
            return res
    try:
        err_mess = res.json()['error'].__str__()
    except:
        err_mess = res.content.__str__()
    err_mess += '\nX-Request_id: {}'.format(res.headers.get('X-Request-Id'))
    raise _APIError(err_mess)


def post_request(url, body, headers=None, params=None, use_gzip=False):
    '''Perform a POST request with a predetermined number of retries.'''
    for _ in range(_constants.RETRY_LIMIT + 1):
        try:
            if use_gzip:
                if headers:
                    headers['Content-Encoding'] = 'gzip'
                else:
                    headers = {'Content-Encoding': 'gzip'}
                res = requests.post(url,
                                    data=gzip.compress(json.dumps(body).encode('utf-8')),
                                    headers=headers,
                                    params=params
                                    )
            else:
                res = requests.post(url,
                                    data=json.dumps(body),
                                    headers=headers,
                                    params=params
                                    )
        except Exception as e:
            raise _APIError(e)
        if res.status_code == 200:
            return res
    try:
        err_mess = res.json()['error'].__str__()
    except:
        err_mess = res.content.__str__()
    err_mess += '\nX-Request_id: {}'.format(res.headers.get('X-Request-Id'))
    raise _APIError(err_mess)

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


def _get_first_datapoint(tag, start, end, api_key, project):
    '''Returns the first datapoint of a timeseries in the given interval. If no start is specified, the default is 1 week ago
    '''
    api_key, project = config.get_config_variables(api_key, project)
    tag = tag.replace('/', '%2F')
    url = config.get_base_url() + '/projects/{}/timeseries/data/{}'.format(project, tag)
    params = {
        'limit': 1,
        'start': start,
        'end': end
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = get_request(url, params=params, headers=headers).json()['data']['items'][0]['datapoints']
    if res:
        return int(res[0]['timestamp'])
    return None


class _APIError(Exception):
    pass


class ProgressIndicator():
    '''This class lets the system give the user and indication of how much data remains to be downloaded in the request'''

    def __init__(self, tag_ids, start, end, api_key=None, project=None):
        self.api_key = api_key
        self.project=project
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

    def terminate(self):
        sys.stdout.write('\r' + ' ' * 500 + '\r')

    def _print_progress(self):
        prog = int(math.ceil(self.progress) / 5)
        remainder = 20 - prog
        sys.stdout.write("\rDownloading requested data: {:5.1f}% |{}|".format(int(math.ceil(self.progress)),
                                                                              '|' * prog + ' ' * remainder))
        sys.stdout.flush()

    def _get_start_end(self, tag_ids, start, end):
        from cognite.timeseries import get_latest
        first_timestamp = float("inf")
        for tag in tag_ids:
            if isinstance(tag, str):
                first_datapoint = _get_first_datapoint(tag, start, end, self.api_key, self.project)
            else:
                first_datapoint = _get_first_datapoint(tag['tagId'], start, end, self.api_key, self.project)
            tag_start = float("inf")
            if first_datapoint:
                tag_start = first_datapoint
            if tag_start < first_timestamp:
                first_timestamp = tag_start
        latest_timestamp = 0

        for tag in tag_ids:
            if isinstance(tag, str):
                tag_end = int(get_latest(tag, self.api_key, self.project).to_json()['timestamp'])
            else:
                tag_end = int(get_latest(tag['tagId'], self.api_key, self.project).to_json()['timestamp'])
            if tag_end > latest_timestamp:
                latest_timestamp = tag_end

        if start is None:
            start = first_timestamp
        elif _time_ago_to_ms(start):  # start is specified as string of format '1d-ago'
            start = latest_timestamp - _time_ago_to_ms(start)
        else:
            start = start

        if end is None:
            end = latest_timestamp
        elif _time_ago_to_ms(end):  # end is specified as string of format '1d-ago'
            end = latest_timestamp - _time_ago_to_ms(end)
        else:
            end = end

        return start, end
