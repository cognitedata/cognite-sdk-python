# -*- coding: utf-8 -*-
"""Sequences Module

This module mirrors the Sequences API.

https://doc.cognitedata.com/api/0.6/#tag/Sequences
"""
import json
from typing import List

from cognite import _utils, config
from cognite.v06.dto import Row, Sequence, SequenceDataRequest, SequenceDataResponse


def post_sequences(sequences: List[Sequence], **kwargs):
    """Create a new time series.

    Args:
        sequences (list[v06.dto.Sequence]):  List of sequence data transfer objects to create.

    Keyword Args:
        api_key (str):                       Your api-key.
        project (str):                       Project name.

    Returns:
        The created sequence
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences".format(project)

    # Remove the id field from the sequences to be posted, as including them will lead to 400's since sequences that
    # are not created yet should not have id's yet.
    for sequence in sequences:
        del sequence.id
        for column in sequence.columns:
            del column.id

    body = {"items": [sequence.__dict__ for sequence in sequences]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.post_request(url, body=body, headers=headers)

    json_response = json.loads(res.text)
    the_sequence: dict = json_response["data"]["items"][0]

    return Sequence.from_JSON(the_sequence)


def get_sequence_by_id(id: int, **kwargs):
    """Returns a Sequence object containing the requested sequence.

    Args:
        id (int):       ID of the sequence to look up

    Keyword Arguments:
        api_key (str):  Your api-key.
        project (str):  Project name.

    Returns:
        v06.dto.Sequence: A data object containing the requested sequence.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences/{}".format(project, id)
    headers = {"api-key": api_key, "accept": "application/json"}

    res = _utils.get_request(url=url, headers=headers, cookies=config.get_cookies())

    json_response = json.loads(res.text)
    the_sequence: dict = json_response["data"]["items"][0]

    return Sequence.from_JSON(the_sequence)


def get_sequence_by_external_id(external_id: str, **kwargs):
    """Returns a Sequence object containing the requested sequence.

    Args:
        external_id (int):  External ID of the sequence to look up

    Keyword Arguments:
        api_key (str):      Your api-key.
        project (str):      Project name.

    Returns:
        v06.dto.Sequence: A data object containing the requested sequence.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    params = {"externalId": external_id}

    res = _utils.get_request(url=url, params=params, headers=headers, cookies=config.get_cookies())

    json_response = json.loads(res.text)
    the_sequence: dict = json_response["data"]["items"][0]

    return Sequence.from_JSON(the_sequence)


def delete_sequence_by_id(id: int, **kwargs):
    """Deletes the sequence with the given id.

    Args:
        id (int):       ID of the sequence to delete

    Keyword Arguments:
        api_key (str):  Your api-key.
        project (str):  Project name.

    Returns:
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences/{}".format(project, id)
    headers = {"api-key": api_key, "accept": "application/json"}

    res = _utils.delete_request(url=url, headers=headers, cookies=config.get_cookies())
    return res.json()


def post_data_to_sequence(id: int, rows: List[Row], **kwargs):
    """Posts data to a sequence.

    Args:
        id (int):       ID of the sequence.
        rows (list):    List of rows with the data.

    Keyword Arguments:
        api_key (str):  Your api-key.
        project (str):  Project name.

    Returns:
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences/{}/postdata".format(project, id)

    body = {"items": [{"rows": [row.__dict__ for row in rows]}]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.post_request(url, body=body, headers=headers)

    return res.json()


def get_data_from_sequence(
    id: int,
    inclusive_from: int = None,
    inclusive_to: int = None,
    limit: int = 100,
    column_ids: List[int] = None,
    **kwargs
):
    """Gets data from the given sequence.

    Args:
        id (int):                id of the sequence.
        inclusive_from (int):    Row number to get from (inclusive). If set to None, you'll get data from the first row
                                 that exists.
        inclusive_to (int):      Row number to get to (inclusive). If set to None, you'll get data to the last row that
                                 exists (depending on the limit).
        limit (int):             How many rows to return.
        column_ids (List[int]):  ids of the columns to get data for.

    Keyword Arguments:
        api_key (str):           Your api-key.
        project (str):           Project name.

    Returns:
        v06.dto.Sequence: A data object containing the requested sequence.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/sequences/{}/getdata".format(project, id)
    headers = {"api-key": api_key, "accept": "application/json", "Content-Type": "application/json"}

    sequenceDataRequest: SequenceDataRequest = SequenceDataRequest(
        inclusive_from=inclusive_from, inclusive_to=inclusive_to, limit=limit, column_ids=column_ids or []
    )

    body = {"items": [sequenceDataRequest.__dict__]}

    res = _utils.post_request(url=url, body=body, headers=headers, cookies=config.get_cookies())

    json_response = json.loads(res.text)
    the_data: dict = json_response["data"]["items"][0]

    return SequenceDataResponse.from_JSON(the_data)
