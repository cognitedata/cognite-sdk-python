# -*- coding: utf-8 -*-
import json
from copy import deepcopy
from typing import List

import pandas as pd

from cognite.client._api_client import APIClient, CogniteCollectionResponse, CogniteResource, CogniteResponse


class EventResponse(CogniteResponse):
    """Event Response Object."""

    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.id = item.get("id")
        self.type = item.get("type")
        self.sub_type = item.get("subType")
        self.description = item.get("description")
        self.asset_ids = item.get("assetIds")
        self.created_time = item.get("createdTime")
        self.start_time = item.get("startTime")
        self.end_time = item.get("endTime")
        self.last_updated_time = item.get("lastUpdatedTime")
        self.metadata = item.get("metadata")

    def to_pandas(self):
        event = self.to_json().copy()
        if event.get("metadata"):
            event.update(event.pop("metadata"))

        # Hack to avoid assetIds ending up as first element in dict as from_dict will fail
        asset_ids = event.pop("assetIds")
        df = pd.DataFrame.from_dict(event, orient="index")
        df.loc["assetIds"] = [asset_ids]
        return df


class EventListResponse(CogniteCollectionResponse):
    """Event List Response Object."""

    _RESPONSE_CLASS = EventResponse

    def __init__(self, internal_representation):
        super().__init__(internal_representation)

    def to_pandas(self):
        items = deepcopy(self.to_json())
        for d in items:
            if d.get("metadata"):
                d.update(d.pop("metadata"))
        return pd.DataFrame(items)


class Event(CogniteResource):
    """Data transfer object for events.

    Args:
        start_time (int):       Start time of the event in ms since epoch.
        end_time (int):         End time of the event in ms since epoch.
        description (str):      Textual description of the event.
        type (str):             Type of the event, e.g. 'failure'.
        subtype (str):          Subtype of the event, e.g. 'electrical'.
        metadata (dict):        Customizable extra data about the event.
        asset_ids (list[int]):  List of Asset IDs of related equipments that this event relates to.
    """

    def __init__(
        self, start_time=None, end_time=None, description=None, type=None, subtype=None, metadata=None, asset_ids=None
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.description = description
        self.type = type
        self.subtype = subtype
        self.metadata = metadata
        self.asset_ids = asset_ids


class EventsClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.5", **kwargs)

    def get_event(self, event_id: int) -> EventResponse:
        """Returns a EventResponse containing an event matching the id.

        Args:
            event_id (int):         The event id.

        Returns:
            stable.events.EventResponse: A data object containing the requested event.

        Examples:
            Getting an event::

                client = CogniteClient()
                res = client.events.get_event(123)
                print(res)
        """
        url = "/events/{}".format(event_id)
        res = self._get(url)
        return EventResponse(res.json())

    def get_events(self, type=None, sub_type=None, asset_id=None, **kwargs) -> EventListResponse:
        """Returns an EventListReponse object containing events matching the query.

        Args:
            type (str):             Type (class) of event, e.g. 'failure'.
            sub_type (str):         Sub-type of event, e.g. 'electrical'.
            asset_id (int):         Return events associated with this assetId.
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
            stable.events.EventListResponse: A data object containing the requested event.

        Examples:
            Getting all events of a given type::

                client = CogniteClient()
                res = client.events.get_events(type="a special type", autopaging=True)
                print(res.to_pandas())
        """
        autopaging = kwargs.get("autopaging", False)
        url = "/events"

        params = {
            "type": type,
            "subtype": sub_type,
            "assetId": asset_id,
            "sort": kwargs.get("sort"),
            "cursor": kwargs.get("cursor"),
            "limit": kwargs.get("limit", 25) if not autopaging else self._LIMIT,
            "hasDescription": kwargs.get("has_description"),
            "minStartTime": kwargs.get("min_start_time"),
            "maxStartTime": kwargs.get("max_start_time"),
        }

        res = self._get(url, params=params, autopaging=autopaging)
        return EventListResponse(res.json())

    def post_events(self, events: List[Event]) -> EventListResponse:
        """Adds a list of events and returns an EventListResponse object containing created events.

        Args:
            events (List[stable.events.Event]):    List of events to create.

        Returns:
            stable.events.EventListResponse

        Examples:
            Posting two events and linking them to an asset::

                from cognite.client.stable.events import Event
                client = CogniteClient()

                my_events = [Event(start_time=1, end_time=10, type="workorder", asset_ids=[123]),
                            Event(start_time=11, end_time=20, type="workorder", asset_ids=[123])]
                res = client.events.post_events(my_events)
                print(res)
        """
        url = "/events"
        items = [event.camel_case_dict() for event in events]
        body = {"items": items}
        res = self._post(url, body=body)
        return EventListResponse(res.json())

    def delete_events(self, event_ids: List[int]) -> None:
        """Deletes a list of events.

        Args:
            event_ids (List[int]):    List of ids of events to delete.

        Returns:
            None

        Examples:
            Deleting a list of events::

                client = CogniteClient()
                res = client.events.delete_events(event_ids=[1,2,3,4,5])
        """
        url = "/events/delete"
        body = {"items": event_ids}
        self._post(url, body=body)

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
            description (str):   Prefix and fuzzy search on description.
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
            stable.events.EventListResponse.

        Examples:
            Perform a fuzzy search on event descriptions and get the first 3 results::

                client = CogniteClient()
                res = client.events.search_for_events(description="Something like this", limit=10)
                print(res)
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
            "assetSubtrees": str(asset_subtrees) if asset_subtrees else None,
            "sort": kwargs.get("sort"),
            "dir": kwargs.get("dir"),
            "limit": kwargs.get("limit", self._LIMIT),
            "offset": kwargs.get("offset"),
        }

        res = self._get(url, params=params)
        return EventListResponse(res.json())
