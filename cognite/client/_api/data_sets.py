from cognite.client._api_client import APIClient
from cognite.client.data_classes import DataSet, DataSetAggregate, DataSetFilter, DataSetList, DataSetUpdate
from cognite.client.utils._identifier import IdentifierSequence


class DataSetsAPI(APIClient):
    _RESOURCE_PATH = "/datasets"

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._CREATE_LIMIT = 10

    def __call__(
        self,
        chunk_size=None,
        metadata=None,
        created_time=None,
        last_updated_time=None,
        external_id_prefix=None,
        write_protected=None,
        limit=None,
    ):
        filter = DataSetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=DataSetList, resource_cls=DataSet, method="POST", chunk_size=chunk_size, filter=filter, limit=limit
        )

    def __iter__(self):
        return cast(Iterator[DataSet], self())

    def create(self, data_set):
        return self._create_multiple(list_cls=DataSetList, resource_cls=DataSet, items=data_set)

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=DataSetList, resource_cls=DataSet, identifiers=identifiers)

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=DataSetList, resource_cls=DataSet, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def list(
        self,
        metadata=None,
        created_time=None,
        last_updated_time=None,
        external_id_prefix=None,
        write_protected=None,
        limit=25,
    ):
        filter = DataSetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
        ).dump(camel_case=True)
        return self._list(list_cls=DataSetList, resource_cls=DataSet, method="POST", limit=limit, filter=filter)

    def aggregate(self, filter=None):
        return self._aggregate(filter=filter, cls=DataSetAggregate)

    def update(self, item):
        return self._update_multiple(list_cls=DataSetList, resource_cls=DataSet, update_cls=DataSetUpdate, items=item)
