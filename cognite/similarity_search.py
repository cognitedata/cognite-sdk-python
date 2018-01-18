# -*- coding: utf-8 -*-
"""Similarity Search Module.

This module mirrors the Similarity Search API.
"""
import cognite._utils as _utils
from cognite.config import get_config_variables, get_base_url
from cognite.data_objects import SimilaritySearchObject

def search(input_tags, query_tags, input_interval, query_interval, modes, limit=10, api_key=None, project=None):
    '''Returns patterns within query interval which are similar to input pattern.

    This method allows you to enter an input interval (the desired interval of a timeseries to use as input) and a
    query interval (the desired interval to search for similarities in) and will return the list of intervals which
    are most similar to your input.

    Args:
        input_tags (list):      List of tags corresponding to time-series patterns to search.

        query_tags (list):      List of tags of time-series that we want to search similar patterns on.

        input_interval (tuple): Start and end of input interval. e.g. {'start': 0, 'end: 100}.

        query_interval (tuple): Start and end of input interval. e.g. {'start': 0, 'end: 100}.

        modes (list):           List of modes of searching. Modes could be 'value' or 'pattern'.

        limit (int):            Number of matches to retrieve.

    Returns:
        SimilaritySearchObject: A data object containing the requested data with several getter methods with different
        output formats.
    '''
    api_key, project = get_config_variables(api_key, project)
    url = get_base_url() + '/projects/{}/similaritysearch'.format(project)
    body = {
        'inputTags': input_tags,
        'queryTags': query_tags,
        'inputInterval': {'start': input_interval[0],
                          'end': input_interval[1]} if isinstance(input_interval, tuple) else input_interval,
        'queryInterval': {'start': query_interval[0],
                          'end': query_interval[1]} if isinstance(query_interval, tuple) else query_interval,
        'modes': modes,
        'limit': limit
    }
    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return SimilaritySearchObject(res.json())
