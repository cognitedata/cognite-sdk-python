"""
===============================================================================
c9ad6444c7e9b577c7eadb5b29053fc1
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.data_classes.annotations import AnnotationReverseLookupFilter, AnnotationWrite
from cognite.client.data_classes.contextualization import ResourceReferenceList
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncAnnotationsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def create(self, annotations: Annotation | AnnotationWrite) -> Annotation: ...

    @overload
    def create(self, annotations: Sequence[Annotation | AnnotationWrite]) -> AnnotationList: ...

    def create(
        self, annotations: Annotation | AnnotationWrite | Sequence[Annotation | AnnotationWrite]
    ) -> Annotation | AnnotationList:
        """
        `Create annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsCreate>`_

        Args:
            annotations (Annotation | AnnotationWrite | Sequence[Annotation | AnnotationWrite]): Annotation(s) to create

        Returns:
            Annotation | AnnotationList: Created annotation(s)
        """
        return run_sync(self.__async_client.annotations.create(annotations=annotations))

    @overload
    def suggest(self, annotations: Annotation | AnnotationWrite) -> Annotation: ...

    @overload
    def suggest(self, annotations: Sequence[Annotation] | Sequence[AnnotationWrite]) -> AnnotationList: ...

    def suggest(
        self, annotations: Annotation | AnnotationWrite | Sequence[Annotation] | Sequence[AnnotationWrite]
    ) -> Annotation | AnnotationList:
        """
        `Suggest annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsSuggest>`_

        Args:
            annotations (Annotation | AnnotationWrite | Sequence[Annotation] | Sequence[AnnotationWrite]): annotation(s) to suggest. They must have status set to "suggested".

        Returns:
            Annotation | AnnotationList: suggested annotation(s)
        """
        return run_sync(self.__async_client.annotations.suggest(annotations=annotations))

    @overload
    def update(
        self,
        item: Annotation | AnnotationWrite | AnnotationUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Annotation: ...

    @overload
    def update(
        self,
        item: Sequence[Annotation | AnnotationWrite | AnnotationUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> AnnotationList: ...

    def update(
        self,
        item: Annotation
        | AnnotationWrite
        | AnnotationUpdate
        | Sequence[Annotation | AnnotationWrite | AnnotationUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Annotation | AnnotationList:
        """
        `Update annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsUpdate>`_

        Args:
            item (Annotation | AnnotationWrite | AnnotationUpdate | Sequence[Annotation | AnnotationWrite | AnnotationUpdate]): Annotation or list of annotations to update (or patch or list of patches to apply)
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Annotation or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Annotation | AnnotationList: No description.
        """
        return run_sync(self.__async_client.annotations.update(item=item, mode=mode))

    def delete(self, id: int | Sequence[int]) -> None:
        """
        `Delete annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsDelete>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs to be deleted
        """
        return run_sync(self.__async_client.annotations.delete(id=id))

    def retrieve_multiple(self, ids: Sequence[int]) -> AnnotationList:
        """
        `Retrieve annotations by IDs <https://developer.cognite.com/api#tag/Annotations/operation/annotationsByids>`_`

        Args:
            ids (Sequence[int]): list of IDs to be retrieved

        Returns:
            AnnotationList: list of annotations
        """
        return run_sync(self.__async_client.annotations.retrieve_multiple(ids=ids))

    def retrieve(self, id: int) -> Annotation | None:
        """
        `Retrieve an annotation by id <https://developer.cognite.com/api#tag/Annotations/operation/annotationsGet>`_

        Args:
            id (int): id of the annotation to be retrieved

        Returns:
            Annotation | None: annotation requested
        """
        return run_sync(self.__async_client.annotations.retrieve(id=id))

    def reverse_lookup(self, filter: AnnotationReverseLookupFilter, limit: int | None = None) -> ResourceReferenceList:
        """
        Reverse lookup annotated resources based on having annotations matching the filter.

        Args:
            filter (AnnotationReverseLookupFilter): Filter to apply
            limit (int | None): Maximum number of results to return. Defaults to None (all).

        Returns:
            ResourceReferenceList: List of resource references

        Examples:

            Retrieve the first 100 ids of annotated resources mathing the 'file' resource type:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AnnotationReverseLookupFilter
                >>> client = CogniteClient()
                >>> flt = AnnotationReverseLookupFilter(annotated_resource_type="file")
                >>> res = client.annotations.reverse_lookup(flt, limit=100)
        """
        return run_sync(self.__async_client.annotations.reverse_lookup(filter=filter, limit=limit))

    def list(self, filter: AnnotationFilter | dict, limit: int | None = DEFAULT_LIMIT_READ) -> AnnotationList:
        """
        `List annotations. <https://developer.cognite.com/api#tag/Annotations/operation/annotationsFilter>`_

        Note:
            Passing a filter with both 'annotated_resource_type' and 'annotated_resource_ids' is always required.

        Args:
            filter (AnnotationFilter | dict): Return annotations with parameter values that match what is specified.
            limit (int | None): Maximum number of annotations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            AnnotationList: list of annotations

        Example:

            List all annotations for the file with id=123:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AnnotationFilter
                >>> client = CogniteClient()
                >>> flt = AnnotationFilter(annotated_resource_type="file", annotated_resource_ids=[{"id": 123}])
                >>> res = client.annotations.list(flt, limit=None)
        """
        return run_sync(self.__async_client.annotations.list(filter=filter, limit=limit))
