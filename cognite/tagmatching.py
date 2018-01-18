# -*- coding: utf-8 -*-
"""Tag Matching Module

This module mirrors the Tag Matching API. It allows the user to search for tag id matches.
"""
import cognite.config as config
import cognite._utils as _utils

from cognite.data_objects import TagMatchingObject

def tag_matching(tag_ids, fuzzy_threshold=0, platform=None, api_key=None, project=None):
    '''Returns a TagMatchingObject containing a list of matched tags for the given query.

    This method takes an arbitrary string as argument and performs fuzzy matching with a user defined threshold
    toward tag ids in the system.

    Args:
        tag_ids (list):         The tag_ids to retrieve matches for.

        fuzzy_threshold (int):  The threshold to use when searching for matches. A fuzzy threshold of 0 means you only
                                want to accept perfect matches. Must be >= 0.

        platform (str):         The platform to search on.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        TagMatchingObject: A data object containing the requested data with several getter methods with different
        output formats.
    '''
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/tagmatching'.format(project)
    body = {
        'tagIds': tag_ids,
        'metadata': {
            'fuzzyThreshold': fuzzy_threshold,
            'platform': platform
        }
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    res = _utils.post_request(url=url, body=body, headers=headers)
    return TagMatchingObject(res.json())
