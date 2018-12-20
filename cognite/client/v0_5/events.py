# -*- coding: utf-8 -*-
"""Events Module

This module mirrors the Events API. It allows you to get, post, update, and delete events.

https://doc.cognitedata.com/0.5/#Cognite-API-Events
"""
import json
from copy import deepcopy
from typing import Dict, List

import pandas as pd

from cognite.client._api_client import APIClient
from cognite.client.v0_5 import CogniteResponse


class EventResponse(CogniteResponse):
    """Event Response Object."""

    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.internal_representation["data"]["items"][0]
        self.id = item.get("id")
        self.asset_ids = item.get("assetIds")

    def to_json(self):
        return self.internal_representation["data"]["items"][0]

    def to_pandas(self):
        event = deepcopy(self.to_json())
        if event.get("metadata"):
            event.update(event.pop("metadata"))
        return pd.DataFrame.from_dict(event, orient="index")


class EventListResponse(CogniteResponse):
    """Event List Response Object."""

    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        self.counter = 0

    def to_json(self):
        return self.internal_representation["data"]["items"]

    def to_pandas(self):
        items = deepcopy(self.to_json())
        for d in items:
            if d.get("metadata"):
                d.update(d.pop("metadata"))
        return pd.DataFrame(items)

    def __iter__(self):
        return self

    def __next__(self):
        if self.counter > len(self.to_json()) - 1:
            raise StopIteration
        else:
            self.counter += 1
            return EventResponse({"data": {"items": [self.to_json()[self.counter - 1]]}})


class Event(object):
    """Data transfer object for events.

    Args:
        start_time (int):       Start time of the event in ms since epoch.
        end_time (int):         End time of the event in ms since epoch.
        description (str):      Textual description of the event.
        type (str):             Type of the event, e.g. 'failure'.
        sub_type (str):          Subtype of the event, e.g. 'electrical'.
        metadata (dict):        Customizable extra data about the event.
        asset_ids (list[int]):  List of Asset IDs of related equipments that this event relates to.
    """

    def __init__(
        self, start_time=None, end_time=None, description=None, type=None, sub_type=None, metadata=None, asset_ids=None
    ):
        self.startTime = start_time
        self.endTime = end_time
        self.description = description
        self.type = type
        self.subtype = sub_type
        self.metadata = metadata
        self.assetIds = asset_ids


class EventsClientV0_5(APIClient):
    def get_event(self, event_id: int, **kwargs) -> EventResponse:
        """Returns a EventResponse containing an event matching the id.

        Args:
            event_id (int):         The event id.

        Returns:
            v0_5.events.EventResponse: A data object containing the requested event.
        """
        url = "/events/{}".format(event_id)
        res = self._get(url)
        return EventResponse(res.json())

    def get_events(self, type=None, sub_type=None, asset_id=None, **kwargs) -> EventListResponse:
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
            autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                    disregarded. Defaults to False.

        Returns:
            v0_5.events.EventListResponse: A data object containing the requested event.
        """
        url = "/events"

        if asset_id:
            params = {
                "assetId": asset_id,
                "sort": kwargs.get("sort"),
                "cursor": kwargs.get("cursor"),
                "limit": kwargs.get("limit", 25) if not kwargs.get("autopaging") else self.LIMIT_AGG,
            }
        else:
            params = {
                "type": type,
                "subtype": sub_type,
                "assetId": asset_id,
                "sort": kwargs.get("sort"),
                "cursor": kwargs.get("cursor"),
                "limit": kwargs.get("limit", 25) if not kwargs.get("autopaging") else self.LIMIT_AGG,
                "hasDescription": kwargs.get("has_description"),
                "minStartTime": kwargs.get("min_start_time"),
                "maxStartTime": kwargs.get("max_start_time"),
            }

        res = self._get(url, params=params)
        events = []
        events.extend(res.json()["data"]["items"])
        next_cursor = res.json()["data"].get("nextCursor")

        while next_cursor and kwargs.get("autopaging"):
            params["cursor"] = next_cursor
            res = self._get(url=url, params=params)
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

    def post_events(self, events: List[Event]) -> EventListResponse:
        """Adds a list of events and returns an EventListResponse object containing created events.

        Args:
            events (List[v0_5.events.Event]):    List of events to create.

        Returns:
            v0_5.events.EventListResponse
        """
        url = "/events"
        body = {"items": [event.__dict__ for event in events]}
        res = self._post(url, body=body)
        return EventListResponse(res.json())

    def delete_events(self, event_ids: List[int]) -> Dict:
        """Deletes a list of events.

        Args:
            event_ids (List[int]):    List of ids of events to delete.

        Returns:
            An empty response.
        """
        url = "/events/delete"
        body = {"items": event_ids}
        res = self._post(url, body=body)
        return res.json()

    def search_for_events(
        self,
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
                v0_5.events.EventListResponse.
            """
        url = "/events/search"
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

        res = self._get(url, params=params)
        return EventListResponse(res.json())
