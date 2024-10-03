from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.data_classes._base import CogniteResource, PropertySpec
from cognite.client.data_classes.annotations import AnnotationCore, AnnotationReverseLookupFilter, AnnotationWrite
from cognite.client.data_classes.contextualization import ResourceReference, ResourceReferenceList
from cognite.client.utils._auxiliary import is_unlimited, split_into_chunks
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._text import convert_all_keys_to_camel_case
from cognite.client.utils._validation import assert_type

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class AnnotationsAPI(APIClient):
    _RESOURCE_PATH = "/annotations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._reverse_lookup_warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="beta", feature_name="Annotation reverse lookup"
        )

    @overload
    def create(self, annotations: Annotation | AnnotationWrite) -> Annotation: ...

    @overload
    def create(self, annotations: Sequence[Annotation | AnnotationWrite]) -> AnnotationList: ...

    def create(
        self, annotations: Annotation | AnnotationWrite | Sequence[Annotation | AnnotationWrite]
    ) -> Annotation | AnnotationList:
        """`Create annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsCreate>`_

        Args:
            annotations (Annotation | AnnotationWrite | Sequence[Annotation | AnnotationWrite]): Annotation(s) to create

        Returns:
            Annotation | AnnotationList: Created annotation(s)
        """
        assert_type(annotations, "annotations", [AnnotationCore, Sequence])

        return self._create_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            resource_path=self._RESOURCE_PATH + "/",
            items=annotations,
            input_resource_cls=AnnotationWrite,
        )

    @overload
    def suggest(self, annotations: Annotation) -> Annotation: ...

    @overload
    def suggest(self, annotations: Sequence[Annotation]) -> AnnotationList: ...

    def suggest(self, annotations: Annotation | Sequence[Annotation]) -> Annotation | AnnotationList:
        """`Suggest annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsSuggest>`_

        Args:
            annotations (Annotation | Sequence[Annotation]): annotation(s) to suggest. They must have status set to "suggested".

        Returns:
            Annotation | AnnotationList: suggested annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, Sequence])
        # Deal with status fields in both cases: Single item and list of items
        items: list[dict[str, Any]] | dict[str, Any] = (
            [self._sanitize_suggest_item(ann) for ann in annotations]
            if isinstance(annotations, Sequence)
            else self._sanitize_suggest_item(annotations)
        )
        return self._create_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            resource_path=self._RESOURCE_PATH + "/suggest",
            items=items,
        )

    @staticmethod
    def _sanitize_suggest_item(annotation: Annotation | dict[str, Any]) -> dict[str, Any]:
        # Check that status is set to suggested if it is set and afterwards remove it
        item = annotation.dump(camel_case=True) if isinstance(annotation, Annotation) else deepcopy(annotation)
        if "status" in item:
            if item["status"] != "suggested":
                raise ValueError("status field for Annotation suggestions must be set to 'suggested'")
            del item["status"]
        return item

    @classmethod
    def _convert_resource_to_patch_object(
        cls,
        resource: CogniteResource,
        update_attributes: list[PropertySpec],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> dict[str, dict[str, dict]]:
        if not isinstance(resource, Annotation):
            return APIClient._convert_resource_to_patch_object(resource, update_attributes)
        annotation: Annotation = resource

        assert annotation.id is not None
        annotation_update = AnnotationUpdate(id=annotation.id)
        for attr in update_attributes:
            getattr(annotation_update, attr.name).set(getattr(annotation, attr.name))
        return annotation_update.dump()

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
        """`Update annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsUpdate>`_

        Args:
            item (Annotation | AnnotationWrite | AnnotationUpdate | Sequence[Annotation | AnnotationWrite | AnnotationUpdate]): Annotation or list of annotations to update (or patch or list of patches to apply)
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Annotation or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Annotation | AnnotationList: No description."""
        return self._update_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, update_cls=AnnotationUpdate, items=item, mode=mode
        )

    def delete(self, id: int | Sequence[int]) -> None:
        """`Delete annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsDelete>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs to be deleted
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)

    def retrieve_multiple(self, ids: Sequence[int]) -> AnnotationList:
        """`Retrieve annotations by IDs <https://developer.cognite.com/api#tag/Annotations/operation/annotationsByids>`_`

        Args:
            ids (Sequence[int]): list of IDs to be retrieved

        Returns:
            AnnotationList: list of annotations
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=None)
        return self._retrieve_multiple(list_cls=AnnotationList, resource_cls=Annotation, identifiers=identifiers)

    def retrieve(self, id: int) -> Annotation | None:
        """`Retrieve an annotation by id <https://developer.cognite.com/api#tag/Annotations/operation/annotationsGet>`_

        Args:
            id (int): id of the annotation to be retrieved

        Returns:
            Annotation | None: annotation requested
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None).as_singleton()
        return self._retrieve_multiple(list_cls=AnnotationList, resource_cls=Annotation, identifiers=identifiers)

    def reverse_lookup(self, filter: AnnotationReverseLookupFilter, limit: int | None = None) -> ResourceReferenceList:
        """Reverse lookup annotated resources based on having annotations matching the filter.

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
        self._reverse_lookup_warning.warn()
        assert_type(filter, "filter", types=[AnnotationReverseLookupFilter], allow_none=False)

        return self._list(
            list_cls=ResourceReferenceList,
            resource_cls=ResourceReference,
            method="POST",
            limit=limit,
            filter=filter.dump(camel_case=True),
            url_path=self._RESOURCE_PATH + "/reverselookup",
            api_subversion="beta",
        )

    def list(self, filter: AnnotationFilter | dict, limit: int | None = DEFAULT_LIMIT_READ) -> AnnotationList:
        """`List annotations. <https://developer.cognite.com/api#tag/Annotations/operation/annotationsFilter>`_

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
        assert_type(filter, "filter", [AnnotationFilter, dict], allow_none=False)

        if isinstance(filter, AnnotationFilter):
            filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            filter = convert_all_keys_to_camel_case(filter)

        if "annotatedResourceIds" not in filter or "annotatedResourceType" not in filter:
            raise ValueError("Both 'annotated_resource_type' and 'annotated_resource_ids' are required in filter!")
        res_ids = list(map(convert_all_keys_to_camel_case, filter.pop("annotatedResourceIds")))

        remaining_limit = limit
        is_finite_limit = not is_unlimited(limit)

        all_annots = AnnotationList([], cognite_client=self._cognite_client)
        for id_chunk in split_into_chunks(res_ids, 1000):
            filter["annotatedResourceIds"] = id_chunk
            chunk_result = self._list(
                list_cls=AnnotationList, resource_cls=Annotation, method="POST", limit=remaining_limit, filter=filter
            )
            all_annots.extend(chunk_result)

            if is_finite_limit:
                remaining_limit = cast(int, limit) - len(all_annots)
                if remaining_limit == 0:
                    break
        return all_annots
