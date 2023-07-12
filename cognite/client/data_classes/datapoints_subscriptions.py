from __future__ import annotations

from typing import Sequence

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import exactly_one_is_not_none

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
    """A data point subscription is a way to listen to changes to time series data points, in ingestion order.
        This is the read version of a subscription, used when reading subscriptions from CDF.

    Args:
        external_id (str): Externally provided ID for the subscription. Must be unique.
        partition_count (int): The maximum effective parallelism of this subscription (the number of clients that can
                               read from it concurrently) will be limited to this number, but a higher partition count
                               will cause a higher time overhead.
        created_time (int): Time when the subscription was created in CDF in milliseconds since Jan 1, 1970.
        last_updated_time (int): Time when the subscription was last updated in CDF in milliseconds since Jan 1, 1970.
        time_series_count (int): The number of time series in the subscription.
        filter (Filter): If present, the subscription is defined by this filter.
        name (str) Human readable name of the subscription.
        description (str): A summary explanation for the subscription.
    """

    def __init__(
        self,
        external_id: str,
        partition_count: int,
        created_time: int,
        last_updated_time: int,
        time_series_count: Sequence[ExternalId] = None,
        filter: str = None,
        name: str = None,
        description: str = None,
        **_: dict,
    ):
        super().__init__(external_id, partition_count, name, description, time_series_count)
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.filter = filter


class DataPointSubscriptionCreate(DatapointSubscriptionCore):
    """A data point subscription is a way to listen to changes to time series data points, in ingestion order.
        This is the write version of a subscription, used to create new subscriptions.

    A subscription can either be defined directly by a list of time series ids or indirectly by a filter.

    Args:
        external_id (str): Externally provided ID for the subscription. Must be unique.
        partition_count (int): The maximum effective parallelism of this subscription (the number of clients that can
                               read from it concurrently) will be limited to this number, but a higher partition count
                               will cause a higher time overhead. The partition count must be between 1 and 100.
                               CAVEAT: This cannot change after the subscription has been created.
        times_series_ids (list[str): List of (external) ids of time series that this subscription will listen to.
                                     Not compatible with filter.
        filter (Filter): A filter DSL (Domain Specific Language) to define advanced filter queries. Not compatible
                         with time_series_ids.
        name (str) Human readable name of the subscription.
        description (str): A summary explanation for the subscription.
    """

    def __init__(
        self,
        external_id: str,
        partition_count: int,
        time_series_ids: Sequence[ExternalId] = None,
        filter: dict = None,
        name: str = None,
        description: str = None,
    ):
        if not exactly_one_is_not_none(time_series_ids, filter):
            raise ValueError("Exactly one of time_series_ids and filter must be given")
        super().__init__(external_id, partition_count, name, description, time_series_ids)
        self.filter = filter


class DataPointSubscriptionUpdate(CogniteResource):
    ...


class DatapointSubscriptionList(CogniteResourceList[DatapointSubscription]):
    _RESOURCE = DatapointSubscription


class DatapointSubscriptionCreateList(CogniteResourceList[DataPointSubscriptionCreate]):
    _RESOURCE = DataPointSubscriptionCreate
