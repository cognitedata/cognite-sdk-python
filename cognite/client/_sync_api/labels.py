"""
===============================================================================
765bcd562409805c3100a5a77572f779
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    LabelDefinition,
    LabelDefinitionList,
    LabelDefinitionWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncLabelsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
    ) -> Iterator[LabelDefinition]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
    ) -> Iterator[LabelDefinitionList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
    ) -> Iterator[LabelDefinition] | Iterator[LabelDefinitionList]:
        """
        Iterate over Labels

        Args:
            chunk_size (int | None): Number of Labels to return in each chunk. Defaults to yielding one Label a time.
            name (str | None): returns the label definitions matching that name
            external_id_prefix (str | None): filter label definitions with external ids starting with the prefix specified
            limit (int | None): Maximum number of label definitions to return. Defaults return all labels.
            data_set_ids (int | Sequence[int] | None): return only labels in the data sets with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): return only labels in the data sets with this external id / these external ids.

        Yields:
            LabelDefinition | LabelDefinitionList: yields Labels one by one or in chunks.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.labels(
                chunk_size=chunk_size,
                name=name,
                external_id_prefix=external_id_prefix,
                limit=limit,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
            )
        )  # type: ignore [misc]

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: Literal[True]) -> LabelDefinition | None: ...

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: Literal[False] = False) -> LabelDefinition: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> LabelDefinitionList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> LabelDefinition | LabelDefinitionList | None:
        """
        `Retrieve one or more label definitions by external id. <https://api-docs.cognite.com/20230101/tag/Labels/operation/byIdsLabels>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external ids
            ignore_unknown_ids (bool): If True, ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            LabelDefinition | LabelDefinitionList | None: The requested label definition(s)

        Examples:

            Get label by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.labels.retrieve(external_id="my_label", ignore_unknown_ids=True)
        """
        return run_sync(
            self.__async_client.labels.retrieve(
                external_id=external_id,  # type: ignore [arg-type]
                ignore_unknown_ids=ignore_unknown_ids,
            )
        )

    def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> LabelDefinitionList:
        """
        `List Labels <https://api-docs.cognite.com/20230101/tag/Labels/operation/listLabels>`_

        Args:
            name (str | None): returns the label definitions matching that name
            external_id_prefix (str | None): filter label definitions with external ids starting with the prefix specified
            data_set_ids (int | Sequence[int] | None): return only labels in the data sets with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): return only labels in the data sets with this external id / these external ids.
            limit (int | None): Maximum number of label definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            LabelDefinitionList: List of requested Labels

        Examples:

            List Labels and filter on name:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> label_list = client.labels.list(limit=5, name="Pump")

            Iterate over label definitions, one-by-one:

                >>> for label in client.labels():
                ...     label  # do something with the label definition

            Iterate over chunks of label definitions to reduce memory load:

                >>> for label_list in client.labels(chunk_size=2500):
                ...     label_list # do something with the type definitions
        """
        return run_sync(
            self.__async_client.labels.list(
                name=name,
                external_id_prefix=external_id_prefix,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                limit=limit,
            )
        )

    @overload
    def create(self, label: LabelDefinition | LabelDefinitionWrite) -> LabelDefinition: ...

    @overload
    def create(self, label: Sequence[LabelDefinition | LabelDefinitionWrite]) -> LabelDefinitionList: ...

    def create(
        self, label: LabelDefinition | LabelDefinitionWrite | Sequence[LabelDefinition | LabelDefinitionWrite]
    ) -> LabelDefinition | LabelDefinitionList:
        """
        `Create one or more label definitions. <https://api-docs.cognite.com/20230101/tag/Labels/operation/createLabelDefinitions>`_

        Args:
            label (LabelDefinition | LabelDefinitionWrite | Sequence[LabelDefinition | LabelDefinitionWrite]): The label definition(s) to create.

        Returns:
            LabelDefinition | LabelDefinitionList: Created label definition(s)

        Raises:
            TypeError: Function input 'label' is of the wrong type

        Examples:

            Create new label definitions:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelDefinitionWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> labels = [LabelDefinitionWrite(external_id="ROTATING_EQUIPMENT", name="Rotating equipment"), LabelDefinitionWrite(external_id="PUMP", name="pump")]
                >>> res = client.labels.create(labels)
        """
        return run_sync(self.__async_client.labels.create(label=label))

    def delete(self, external_id: str | SequenceNotStr[str] | None = None) -> None:
        """
        `Delete one or more label definitions <https://api-docs.cognite.com/20230101/tag/Labels/operation/deleteLabels>`_

        Args:
            external_id (str | SequenceNotStr[str] | None): One or more label external ids

        Examples:

            Delete label definitions by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.labels.delete(external_id=["big_pump", "small_pump"])
        """
        return run_sync(self.__async_client.labels.delete(external_id=external_id))
