import copy
from typing import Iterator, List, Sequence, cast

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Relationship, RelationshipFilter, RelationshipList, RelationshipUpdate
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._identifier import IdentifierSequence


class RelationshipsAPI(APIClient):
    _RESOURCE_PATH = "/relationships"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._CREATE_LIMIT = 1000
        self._LIST_SUBQUERY_LIMIT = 1000

    def _create_filter(
        self,
        source_external_ids=None,
        source_types=None,
        target_external_ids=None,
        target_types=None,
        data_set_ids=None,
        start_time=None,
        end_time=None,
        confidence=None,
        last_updated_time=None,
        created_time=None,
        active_at_time=None,
        labels=None,
    ):
        return RelationshipFilter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        ).dump(camel_case=True)

    def __call__(
        self,
        source_external_ids=None,
        source_types=None,
        target_external_ids=None,
        target_types=None,
        data_set_ids=None,
        data_set_external_ids=None,
        start_time=None,
        end_time=None,
        confidence=None,
        last_updated_time=None,
        created_time=None,
        active_at_time=None,
        labels=None,
        limit=None,
        fetch_resources=False,
        chunk_size=None,
        partitions=None,
    ):
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = self._create_filter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids_processed,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        )
        if (len(filter.get("targetExternalIds", [])) > self._LIST_SUBQUERY_LIMIT) or (
            len(filter.get("sourceExternalIds", [])) > self._LIST_SUBQUERY_LIMIT
        ):
            raise ValueError(
                f"For queries with more than {self._LIST_SUBQUERY_LIMIT} source_external_ids or target_external_ids, only list is supported"
            )
        return self._list_generator(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            method="POST",
            limit=limit,
            filter=filter,
            chunk_size=chunk_size,
            partitions=partitions,
            other_params={"fetchResources": fetch_resources},
        )

    def __iter__(self):
        return cast(Iterator[Relationship], self())

    def retrieve(self, external_id, fetch_resources=False):
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            identifiers=identifiers,
            other_params={"fetchResources": fetch_resources},
        )

    def retrieve_multiple(self, external_ids, fetch_resources=False):
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            identifiers=identifiers,
            other_params={"fetchResources": fetch_resources},
        )

    def list(
        self,
        source_external_ids=None,
        source_types=None,
        target_external_ids=None,
        target_types=None,
        data_set_ids=None,
        data_set_external_ids=None,
        start_time=None,
        end_time=None,
        confidence=None,
        last_updated_time=None,
        created_time=None,
        active_at_time=None,
        labels=None,
        limit=100,
        partitions=None,
        fetch_resources=False,
    ):
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = self._create_filter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids_processed,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
        )
        target_external_id_list: List[str] = filter.get("targetExternalIds", [])
        source_external_id_list: List[str] = filter.get("sourceExternalIds", [])
        if (len(target_external_id_list) > self._LIST_SUBQUERY_LIMIT) or (
            len(source_external_id_list) > self._LIST_SUBQUERY_LIMIT
        ):
            if not is_unlimited(limit):
                raise ValueError(
                    f"Querying more than {self._LIST_SUBQUERY_LIMIT} source_external_ids/target_external_ids only supported for queries without limit (pass -1 / None / inf instead of {limit})"
                )
            tasks = []
            for ti in range(0, max(1, len(target_external_id_list)), self._LIST_SUBQUERY_LIMIT):
                for si in range(0, max(1, len(source_external_id_list)), self._LIST_SUBQUERY_LIMIT):
                    task_filter = copy.copy(filter)
                    if target_external_id_list:
                        task_filter["targetExternalIds"] = target_external_id_list[
                            ti : (ti + self._LIST_SUBQUERY_LIMIT)
                        ]
                    if source_external_id_list:
                        task_filter["sourceExternalIds"] = source_external_id_list[
                            si : (si + self._LIST_SUBQUERY_LIMIT)
                        ]
                    tasks.append((task_filter,))
            tasks_summary = utils._concurrency.execute_tasks(
                (
                    lambda filter: self._list(
                        list_cls=RelationshipList,
                        resource_cls=Relationship,
                        method="POST",
                        limit=limit,
                        filter=filter,
                        other_params={"fetchResources": fetch_resources},
                        partitions=partitions,
                    )
                ),
                tasks,
                max_workers=self._config.max_workers,
            )
            if tasks_summary.exceptions:
                raise tasks_summary.exceptions[0]
            return RelationshipList(tasks_summary.joined_results())
        return self._list(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            method="POST",
            limit=limit,
            filter=filter,
            other_params={"fetchResources": fetch_resources},
        )

    def create(self, relationship):
        utils._auxiliary.assert_type(relationship, "relationship", [Relationship, Sequence])
        if isinstance(relationship, Sequence):
            relationship = [r._validate_resource_types() for r in relationship]
        else:
            relationship = relationship._validate_resource_types()
        return self._create_multiple(list_cls=RelationshipList, resource_cls=Relationship, items=relationship)

    def update(self, item):
        return self._update_multiple(
            list_cls=RelationshipList, resource_cls=Relationship, update_cls=RelationshipUpdate, items=item
        )

    def delete(self, external_id, ignore_unknown_ids=False):
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )
