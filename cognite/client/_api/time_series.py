from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    TimeSeries,
    TimeSeriesAggregate,
    TimeSeriesFilter,
    TimeSeriesList,
    TimeSeriesUpdate,
)
from cognite.client.utils._identifier import IdentifierSequence


class TimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/timeseries"

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.data = DatapointsAPI(*args, **kwargs)

    def __call__(
        self,
        chunk_size=None,
        name=None,
        unit=None,
        is_string=None,
        is_step=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        metadata=None,
        external_id_prefix=None,
        created_time=None,
        last_updated_time=None,
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
        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            metadata=metadata,
            created_time=created_time,
            data_set_ids=data_set_ids_processed,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            partitions=partitions,
        )

    def __iter__(self):
        return cast(Iterator[TimeSeries], self())

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=TimeSeriesList, resource_cls=TimeSeries, identifiers=identifiers)

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(
        self,
        name=None,
        unit=None,
        is_string=None,
        is_step=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        metadata=None,
        external_id_prefix=None,
        created_time=None,
        last_updated_time=None,
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
        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            metadata=metadata,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            filter=filter,
            limit=limit,
            partitions=partitions,
        )

    def aggregate(self, filter=None):
        return self._aggregate(filter=filter, cls=TimeSeriesAggregate)

    @overload
    def create(self, time_series):
        ...

    @overload
    def create(self, time_series):
        ...

    def create(self, time_series):
        return self._create_multiple(list_cls=TimeSeriesList, resource_cls=TimeSeries, items=time_series)

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
        return self._update_multiple(
            list_cls=TimeSeriesList, resource_cls=TimeSeries, update_cls=TimeSeriesUpdate, items=item
        )

    def search(self, name=None, description=None, query=None, filter=None, limit=100):
        return self._search(
            list_cls=TimeSeriesList,
            search={"name": name, "description": description, "query": query},
            filter=(filter or {}),
            limit=limit,
        )
