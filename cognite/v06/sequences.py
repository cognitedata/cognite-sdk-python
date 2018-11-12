# -*- coding: utf-8 -*-
"""Sequences Module

This module mirrors the Sequences API.

https://doc.cognitedata.com/api/0.6/#tag/Sequences
"""
import json
from typing import List

from cognite import _utils, config
from cognite.v06.dto import Sequence


def post_sequences(
        sequences: List[Sequence],
        **kwargs
):
    """Create a new time series.

    Args:
        sequences (list[v06.dto.Sequence]): List of sequence data transfer objects to create.

    Keyword Args:
        api_key (str): Your api-key.
        project (str): Project name.

    Returns:
        The created sequence
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences".format(project)

    body = {"items": [sequence.__dict__ for sequence in sequences]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.post_request(url, body=body, headers=headers)

    json_response = json.loads(res.text)
    the_sequence: dict = json_response['data']['items'][0]

    return Sequence.from_JSON(the_sequence)


def get_sequence_by_id(
        id: int,
        **kwargs
):
    """Returns a Sequence object containing the requested sequence.

    Args:
        id (int):                 ID of the sequence to look up

    Keyword Arguments:
        api_key (str):            Your api-key.
        project (str):            Project name.

    Returns:
        v06.dto.Sequence: A data object containing the requested sequence.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences/{}".format(project, id)
    headers = {"api-key": api_key, "accept": "application/json"}

    res = _utils.get_request(url=url, headers=headers, cookies=config.get_cookies())

    json_response = json.loads(res.text)
    the_sequence: dict = json_response['data']['items'][0]

    return Sequence.from_JSON(the_sequence)
