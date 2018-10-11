# -*- coding: utf-8 -*-
"""Events Module

This module mirrors the Events API. It allows you to get, post, update, and delete events.

https://doc.cognitedata.com/0.5/#Cognite-API-Events
"""
import json

from cognite import _constants, _utils, config
from cognite.v05.dto import EventListResponse, EventResponse


def get_event(event_id, **kwargs):
    """Returns a EventResponse containing an event matching the id.

    Args:
        event_id (int):         The event id.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        v05.dto.EventResponse: A data object containing the requested event.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/events/{}".format(project, event_id)
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    res = _utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return EventResponse(res.json())


def get_events(type=None, sub_type=None, asset_id=None, **kwargs):
    """Returns an EventListReponse object containing events matching the query.

    Args:
        type (str):             Type (class) of event, e.g. 'failure'.
        sub_type (str):         Sub-type of event, e.g. 'electrical'.
        asset_id (str):         Return events associated with this assetId.
    Keyword Arguments:
        sort (str):             Sort descending or ascending. Default 'ASC'.
        cursor (str):           Cursor to use for paging through results.
        limit (int):            Return up to this many results. Maximum is 10000. Default is 25.
        has_description (bool): Return only events that have a textual description. Default null. False gives only
                                those without description.
        min_start_time (string): Only return events from after this time.
        max_start_time (string): Only return events form before this time.
        api_key (str):          Your api-key.
        project (str):          Project name.
        autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                disregarded. Defaults to False.

    Returns:
        v05.dto.EventListResponse: A data object containing the requested event.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/events".format(project)

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    if asset_id:
        params = {
            "assetId": asset_id,
            "sort": kwargs.get("sort"),
            "cursor": kwargs.get("cursor"),
            "limit": kwargs.get("limit", 25) if not kwargs.get("autopaging") else _constants.LIMIT_AGG,
        }
    else:
        params = {
            "type": type,
            "subtype": sub_type,
            "assetId": asset_id,
            "sort": kwargs.get("sort"),
            "cursor": kwargs.get("cursor"),
            "limit": kwargs.get("limit", 25) if not kwargs.get("autopaging") else _constants.LIMIT_AGG,
            "hasDescription": kwargs.get("has_description"),
            "minStartTime": kwargs.get("min_start_time"),
            "maxStartTime": kwargs.get("max_start_time"),
        }

    res = _utils.get_request(url, headers=headers, params=params, cookies=config.get_cookies())
    events = []
    events.extend(res.json()["data"]["items"])
    next_cursor = res.json()["data"].get("nextCursor")

    while next_cursor and kwargs.get("autopaging"):
        params["cursor"] = next_cursor
        res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
        events.extend(res.json()["data"]["items"])
        next_cursor = res.json()["data"].get("nextCursor")

    return EventListResponse(
        {
            "data": {
                "nextCursor": next_cursor,
                "previousCursor": res.json()["data"].get("previousCursor"),
                "items": events,
            }
        }
    )


def post_events(events, **kwargs):
    """Adds a list of events and returns an EventListResponse object containing created events.

    Args:
        events (List[v05.dto.Event]):    List of events to create.

    Keyword Args:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        v05.dto.EventListResponse
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/events".format(project)

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    body = {"items": [event.__dict__ for event in events]}

    res = _utils.post_request(url, body=body, headers=headers)
    return EventListResponse(res.json())


def delete_events(event_ids, **kwargs):
    """Deletes a list of events.

    Args:
        event_ids (List[int]):    List of ids of events to delete.

    Keyword Args:
        api_key (str):          Your api-key.
        project (str):          Project name.

    Returns:
        An empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/events/delete".format(project)

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    body = {"items": event_ids}

    res = _utils.post_request(url, body=body, headers=headers)
    return res.json()


def search_for_events(
    description=None,
    type=None,
    subtype=None,
    min_start_time=None,
    max_start_time=None,
    min_end_time=None,
    max_end_time=None,
    min_created_time=None,
    max_created_time=None,
    min_last_updated_time=None,
    max_last_updated_time=None,
    metadata=None,
    asset_ids=None,
    asset_subtrees=None,
    **kwargs
):
    """Search for events.

        Args:
            description str:   Prefix and fuzzy search on description.

        Keyword Args:
            api_key (str):          Your api-key.
            project (str):          Project name.
            type (str):             Filter on type (case-sensitive).
            subtype (str):          Filter on subtype (case-sensitive).
            min_start_time (str):   Filter out events with startTime before this. Format is milliseconds since epoch.
            max_start_time (str):   Filter out events with startTime after this. Format is milliseconds since epoch.
            min_end_time (str):     Filter out events with endTime before this. Format is milliseconds since epoch.
            max_end_time (str):     Filter out events with endTime after this. Format is milliseconds since epoch.
            min_created_time(str):  Filter out events with createdTime before this. Format is milliseconds since epoch.
            max_created_time (str): Filter out events with createdTime after this. Format is milliseconds since epoch.
            min_last_updated_time(str):  Filter out events with lastUpdatedtime before this. Format is milliseconds since epoch.
            max_last_updated_time(str): Filter out events with lastUpdatedtime after this. Format is milliseconds since epoch.
            metadata (dict):        Filter out events that do not match these metadata fields and values (case-sensitive).
                                    Format is {"key1":"value1","key2":"value2"}.
            asset_ids (List[int]):  Filter out events that are not linked to any of these assets. Format is [12,345,6,7890].
            asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
                                        Format is [12,345,6,7890].

        Keyword Args:
            sort (str):             Field to be sorted.
            dir (str):              Sort direction (desc or asc)
            limit (int):            Return up to this many results. Max is 1000, default is 25.
            offset (int):           Offset from the first result. Sum of limit and offset must not exceed 1000. Default is 0.
        Returns:
            v05.dto.EventListResponse.
        """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/events/search".format(project)
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    params = {
        "description": description,
        "type": type,
        "subtype": subtype,
        "minStartTime": min_start_time,
        "maxStartTime": max_start_time,
        "minEndTime": min_end_time,
        "maxEndTime": max_end_time,
        "minCreatedTime": min_created_time,
        "maxCreatedTime": max_created_time,
        "minLastUpdatedTime": min_last_updated_time,
        "maxLastUpdatedTime": max_last_updated_time,
        "metadata": json.dumps(metadata),
        "assetIds": str(asset_ids or []),
        "assetSubtrees": asset_subtrees,
        "sort": kwargs.get("sort"),
        "dir": kwargs.get("dir"),
        "limit": kwargs.get("limit", 1000),
        "offset": kwargs.get("offset"),
    }

    res = _utils.get_request(url, headers=headers, params=params)

    return EventListResponse(res.json())
