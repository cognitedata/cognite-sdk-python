from typing import Iterator, Sequence, cast

from cognite.client._api_client import APIClient
from cognite.client.data_classes import LabelDefinition, LabelDefinitionFilter, LabelDefinitionList
from cognite.client.utils._identifier import IdentifierSequence


class LabelsAPI(APIClient):
    _RESOURCE_PATH = "/labels"

    def __iter__(self):
        return cast(Iterator[LabelDefinition], self())

    def __call__(
        self,
        name=None,
        external_id_prefix=None,
        limit=None,
        chunk_size=None,
        data_set_ids=None,
        data_set_external_ids=None,
    ):
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            method="POST",
            limit=limit,
            filter=filter,
            chunk_size=chunk_size,
        )

    def list(self, name=None, external_id_prefix=None, data_set_ids=None, data_set_external_ids=None, limit=25):
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)
        return self._list(
            list_cls=LabelDefinitionList, resource_cls=LabelDefinition, method="POST", limit=limit, filter=filter
        )

    def create(self, label):
        if isinstance(label, Sequence):
            if (len(label) > 0) and (not isinstance(label[0], LabelDefinition)):
                raise TypeError("'label' must be of type LabelDefinition or Sequence[LabelDefinition]")
        elif not isinstance(label, LabelDefinition):
            raise TypeError("'label' must be of type LabelDefinition or Sequence[LabelDefinition]")
        return self._create_multiple(list_cls=LabelDefinitionList, resource_cls=LabelDefinition, items=label)

    def delete(self, external_id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(external_ids=external_id), wrap_ids=True)
