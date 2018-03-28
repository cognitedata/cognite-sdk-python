# -*- coding: utf-8 -*-
"""Utilites for Cognite API SDK

This module provides helper methods and different utilities for the Cognite API Python SDK.

This module is protected and should not used by end-users.
"""
import gzip
import json
import re
import sys
import time
from datetime import datetime

import requests

import cognite.config as config


def get_request(url, params=None, headers=None, cookies=None):
    '''Perform a GET request with a predetermined number of retries.'''
    for number_of_tries in range(config.get_number_of_retries() + 1):
        try:
            res = requests.get(url, params=params, headers=headers, cookies=cookies)
            if res.status_code == 200:
                return res
        except Exception as e:
            if number_of_tries == config.get_number_of_retries():
                raise APIError(e)
    try:
        err_mess = res.json()['error'].__str__()
    except:
        err_mess = res.content.__str__()
    err_mess += '\nX-Request_id: {}'.format(res.headers.get('X-Request-Id'))
    raise APIError(err_mess)


def post_request(url, body, headers=None, params=None, cookies=None, use_gzip=False):
    '''Perform a POST request with a predetermined number of retries.'''
    for number_of_tries in range(config.get_number_of_retries() + 1):
        try:
            if use_gzip:
                if headers:
                    headers['Content-Encoding'] = 'gzip'
                else:
                    headers = {'Content-Encoding': 'gzip'}
                res = requests.post(url,
                                    data=gzip.compress(json.dumps(body).encode('utf-8')),
                                    headers=headers,
                                    params=params,
                                    cookies=cookies
                                    )
            else:
                res = requests.post(url,
                                    data=json.dumps(body),
                                    headers=headers,
                                    params=params,
                                    cookies=cookies
                                    )
            if res.status_code == 200:
                return res
        except Exception as e:
            if number_of_tries == config.get_number_of_retries():
                raise APIError(e)
    try:
        err_mess = res.json()['error'].__str__()
    except:
        err_mess = res.content.__str__()
    err_mess += '\nX-Request_id: {}'.format(res.headers.get('X-Request-Id'))
    raise APIError(err_mess)


def put_request(url, body=None, headers=None, cookies=None):
    '''Perform a PUT request with a predetermined number of retries.'''
    for number_of_tries in range(config.get_number_of_retries() + 1):
        try:
            res = requests.put(url, data=json.dumps(body), headers=headers, cookies=cookies)
            if res.ok:
                return res
        except Exception as e:
            if number_of_tries == config.get_number_of_retries():
                raise APIError(e)
    try:
        err_mess = res.json()['error'].__str__()
    except:
        err_mess = res.content.__str__()
    err_mess += '\nX-Request_id: {}'.format(res.headers.get('X-Request-Id'))
    raise APIError(err_mess)


def datetime_to_ms(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000)


def round_to_nearest(x, base):
    return int(base * round(float(x) / base))


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


def interval_to_ms(start, end):
    '''Returns the ms representation of start-end-interval whether it is time-ago, datetime or None.'''
    time_now = int(round(time.time() * 1000))
    if isinstance(start, datetime):
        start = datetime_to_ms(start)
    elif isinstance(start, str):
        start = time_now - _time_ago_to_ms(start)
    elif start is None:
        start = time_now - _time_ago_to_ms('1w-ago')

    if isinstance(end, datetime):
        end = datetime_to_ms(end)
    elif isinstance(end, str):
        end = time_now - _time_ago_to_ms(end)
    elif end is None:
        end = time_now

    return start, end


class APIError(Exception):
    pass


class ProgressIndicator():
    '''This class lets the system give the user that data is being downloaded'''

    def __init__(self, tag_ids):
        sys.stdout.write("\rDownloading data for tags {}...".format(tag_ids))

    def terminate(self):
        sys.stdout.write('\r' + ' ' * 500 + '\r')
