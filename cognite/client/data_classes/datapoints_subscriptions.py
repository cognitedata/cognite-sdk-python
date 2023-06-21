from __future__ import annotations

from typing import Sequence

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

ExternalId = str


class DatapointSubscriptionCore(CogniteResource):
    def __init__(
        self,
        external_id: str,
        partition_count: int,
        name: str = None,
        description: str = None,
        time_series_ids: Sequence[ExternalId] = None,
        **_: dict,
    ):
        self.external_id = external_id
        self.partition_count = partition_count
        self.name = name
        self.description = description
        self.time_series_ids = time_series_ids


class DatapointSubscription(DatapointSubscriptionCore):
    def __init__(
        self,
        external_id: str,
        partition_count: int,
        created_time: int,
        last_updated_time: int,
        name: str = None,
        description: str = None,
        time_series_ids: Sequence[ExternalId] = None,
        filter: str = None,
        **_: dict,
    ):
        super().__init__(external_id, partition_count, name, description, time_series_ids)
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.filter = filter


class DataPointSubscriptionCreate(DatapointSubscriptionCore):
    def __init__(
        self,
        external_id: str,
        partition_count: int,
        name: str = None,
        description: str = None,
        time_series_ids: Sequence[ExternalId] = None,
        filter: dict = None,
    ):
        super().__init__(external_id, partition_count, name, description, time_series_ids)
        self.filter = filter


class DataPointSubscriptionUpdate(CogniteResource):
    ...


class DatapointSubscriptionList(CogniteResourceList[DatapointSubscription]):
    _RESOURCE = DatapointSubscription


class DatapointSubscriptionCreate(CogniteResourceList[DataPointSubscriptionCreate]):
    _RESOURCE = DataPointSubscriptionCreate
