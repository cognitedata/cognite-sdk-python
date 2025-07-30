from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, NoReturn, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteFilter
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.simulators.filters import PropertySort, SimulatorModelRevisionsFilter
from cognite.client.data_classes.simulators.models import (
    SimulatorModelRevision,
    SimulatorModelRevisionList,
    SimulatorModelRevisionWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorModelRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/models/revisions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 100

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        sort: PropertySort | None = None,
        model_external_ids: str | SequenceNotStr[str] | None = None,
        all_versions: bool | None = None,
        created_time: TimestampRange | None = None,
        last_updated_time: TimestampRange | None = None,
    ) -> SimulatorModelRevisionList:
        """`Filter simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/filter_simulator_model_revisions_simulators_models_revisions_list_post>`_

        Retrieves a list of simulator model revisions that match the given criteria.

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.
            sort (PropertySort | None): The criteria to sort by.
            model_external_ids (str | SequenceNotStr[str] | None): The external ids of the simulator models to filter by.
            all_versions (bool | None): If True, all versions of the simulator model revisions are returned. If False, only the latest version is returned.
            created_time (TimestampRange | None): Filter by created time.
            last_updated_time (TimestampRange | None): Filter by last updated time.
        Returns:
            SimulatorModelRevisionList: List of simulator model revisions

        Examples:
            List simulator model revisions:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.list(limit=10)

            Specify filter and sort order:
                >>> from cognite.client.data_classes.simulators import SimulatorModelRevisionsFilter
                >>> from cognite.client.data_classes.simulators.filters import PropertySort
                >>> from cognite.client.data_classes.shared import TimestampRange
                >>> res = client.simulators.models.revisions.list(
                ...     model_external_ids=["model1", "model2"],
                ...     all_versions=True,
                ...     created_time=TimestampRange(min=0, max=1000000),
                ...     last_updated_time=TimestampRange(min=0, max=1000000),
                ...     sort=PropertySort(order="asc", property="createdTime"),
                ...     limit=10
                ... )
        """
        model_revisions_filter = SimulatorModelRevisionsFilter(
            model_external_ids=model_external_ids,
            all_versions=all_versions,
            created_time=created_time,
            last_updated_time=last_updated_time,
        )
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorModelRevision,
            list_cls=SimulatorModelRevisionList,
            sort=[PropertySort.load(sort).dump()] if sort else None,
            filter=model_revisions_filter.dump(),
        )

    @overload
    def retrieve(self, ids: None = None, external_ids: None = None) -> NoReturn: ...

    @overload
    def retrieve(self, ids: int, external_ids: None = None) -> SimulatorModelRevision | None: ...

    @overload
    def retrieve(
        self,
        ids: None,
        external_ids: str,
    ) -> SimulatorModelRevision | None: ...

    @overload
    def retrieve(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModelRevision | SimulatorModelRevisionList | None: ...

    def retrieve(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> SimulatorModelRevision | SimulatorModelRevisionList | None:
        """`Retrieve simulator model revisions <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_

        Retrieve one or more simulator model revisions by ID(s) or external ID(s).

        Args:
            ids (int | Sequence[int] | None): The ids of the simulator model revisions.
            external_ids (str | SequenceNotStr[str] | None): The external ids of the simulator model revisions.

        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList | None: Requested simulator model revision(s)

        Examples:
            Get simulator model revision by id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.models.revisions.retrieve(ids=1)

            Get simulator model revision by external id:
                >>> res = client.simulators.models.revisions.retrieve(
                ...     external_ids="revision_external_id"
                ... )

            Get multiple simulator model revisions by ids:
                >>> res = client.simulators.models.revisions.retrieve(ids=[1,2])

            Get multiple simulator model revisions by external ids:
                >>> res = client.simulators.models.revisions.retrieve(
                ...     external_ids=["revision1", "revision2"]
                ... )
        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
        )

    def __iter__(self) -> Iterator[SimulatorModelRevision]:
        """Iterate over simulator model revisions

        Fetches simulator model revisions as they are iterated over, so you keep a limited number of simulator model revisions in memory.

        Returns:
            Iterator[SimulatorModelRevision]: yields Simulator model revisions one by one.
        """
        return self()

    @overload
    def __call__(
        self, chunk_size: int, filter: SimulatorModelRevisionsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModelRevisionList]: ...

    @overload
    def __call__(
        self, chunk_size: None = None, filter: SimulatorModelRevisionsFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorModelRevision]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        filter: SimulatorModelRevisionsFilter | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorModelRevision] | Iterator[SimulatorModelRevisionList]:
        """Iterate over simulator simulator model revisions

        Fetches simulator model revisions as they are iterated over, so you keep a limited number of simulator model revisions in memory.

        Args:
            chunk_size (int | None): Number of simulator model revisions to return in each chunk. Defaults to yielding one simulator model revision a time.
            filter (SimulatorModelRevisionsFilter | None): Filter to apply on the model revisions list.
            limit (int | None): Maximum number of simulator model revisions to return. Defaults to return all items.

        Returns:
            Iterator[SimulatorModelRevision] | Iterator[SimulatorModelRevisionList]: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        return self._list_generator(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            method="POST",
            filter=filter.dump() if isinstance(filter, CogniteFilter) else filter,
            chunk_size=chunk_size,
            limit=limit,
        )

    @overload
    def create(self, items: SimulatorModelRevisionWrite) -> SimulatorModelRevision: ...

    @overload
    def create(self, items: Sequence[SimulatorModelRevisionWrite]) -> SimulatorModelRevisionList: ...

    def create(
        self, items: SimulatorModelRevisionWrite | Sequence[SimulatorModelRevisionWrite]
    ) -> SimulatorModelRevision | SimulatorModelRevisionList:
        """`Create simulator model revisions <https://api-docs.cognite.com/20230101-beta/tag/Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post>`_

        Args:
            items (SimulatorModelRevisionWrite | Sequence[SimulatorModelRevisionWrite]): The model revision(s) to create.

        Returns:
            SimulatorModelRevision | SimulatorModelRevisionList: Created simulator model revision(s)

        Examples:
            Create new simulator model revisions:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorModelRevisionWrite
                >>> client = CogniteClient()
                >>> revisions = [
                ...     SimulatorModelRevisionWrite(
                ...         external_id="revision1",
                ...         file_id=1,
                ...         model_external_id="a_1",
                ...     ),
                ...     SimulatorModelRevisionWrite(
                ...         external_id="revision2",
                ...         file_id=2,
                ...         model_external_id="a_2",
                ...     ),
                ... ]
                >>> res = client.simulators.models.revisions.create(revisions)
        """
        assert_type(items, "simulator_model_revision", [SimulatorModelRevisionWrite, Sequence])

        return self._create_multiple(
            list_cls=SimulatorModelRevisionList,
            resource_cls=SimulatorModelRevision,
            items=items,
            input_resource_cls=SimulatorModelRevisionWrite,
        )
