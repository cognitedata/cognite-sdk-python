from typing import Iterator, cast, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    AggregateResult,
    AggregateUniqueValuesResult,
    Event,
    EventFilter,
    EventList,
    EventUpdate,
)
from cognite.client.utils._identifier import IdentifierSequence


class EventsAPI(APIClient):
    _RESOURCE_PATH = "/events"

    def __call__(
        self,
        chunk_size=None,
        start_time=None,
        end_time=None,
        active_at_time=None,
        type=None,
        subtype=None,
        metadata=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        external_id_prefix=None,
        sort=None,
        limit=None,
        partitions=None,
    ):
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = EventFilter(
            start_time=start_time,
            end_time=end_time,
            active_at_time=active_at_time,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            type=type,
            subtype=subtype,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            sort=sort,
            partitions=partitions,
        )

    def __iter__(self):
        return cast(Iterator[Event], self())

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=EventList, resource_cls=Event, identifiers=identifiers)

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=EventList, resource_cls=Event, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def list(
        self,
        start_time=None,
        end_time=None,
        active_at_time=None,
        type=None,
        subtype=None,
        metadata=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        external_id_prefix=None,
        sort=None,
        partitions=None,
        limit=25,
    ):
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        if end_time and (("max" in end_time) or ("min" in end_time)) and ("isNull" in end_time):
            raise ValueError("isNull cannot be used with min or max values")
        filter = EventFilter(
            start_time=start_time,
            end_time=end_time,
            active_at_time=active_at_time,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            source=source,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            type=type,
            subtype=subtype,
        ).dump(camel_case=True)
        return self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=partitions,
            sort=sort,
        )

    def aggregate(self, filter=None):
        return self._aggregate(filter=filter, cls=AggregateResult)

    def aggregate_unique_values(self, filter=None, fields=None):
        return self._aggregate(filter=filter, fields=fields, aggregate="uniqueValues", cls=AggregateUniqueValuesResult)

    @overload
    def create(self, event):
        ...

    @overload
    def create(self, event):
        ...

    def create(self, event):
        return self._create_multiple(list_cls=EventList, resource_cls=Event, items=event)

    def delete(self, id=None, external_id=None, ignore_unknown_ids=False):
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(self, item):
        ...

    @overload
    def update(self, item):
        ...

    def update(self, item):
        return self._update_multiple(list_cls=EventList, resource_cls=Event, update_cls=EventUpdate, items=item)

    def search(self, description=None, filter=None, limit=100):
        return self._search(list_cls=EventList, search={"description": description}, filter=(filter or {}), limit=limit)
