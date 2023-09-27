from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.data_classes._base import CogniteResource, PropertySpec
from cognite.client.data_classes.annotations import AnnotationReverseLookupFilter
from cognite.client.data_classes.contextualization import ResourceReference, ResourceReferenceList
from cognite.client.utils._auxiliary import assert_type
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._text import to_camel_case


class AnnotationsAPI(APIClient):
    _RESOURCE_PATH = "/annotations"

    @overload
    def create(self, annotations: Annotation) -> Annotation:
        ...

    @overload
    def create(self, annotations: Sequence[Annotation]) -> AnnotationList:
        ...

    def create(self, annotations: Annotation | Sequence[Annotation]) -> Annotation | AnnotationList:
        """`Create annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsCreate>`_

        Args:
            annotations (Annotation | Sequence[Annotation]): annotation(s) to create

        Returns:
            Annotation | AnnotationList: created annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, Sequence])
        return self._create_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, resource_path=self._RESOURCE_PATH + "/", items=annotations
        )

    @overload
    def suggest(self, annotations: Annotation) -> Annotation:
        ...

    @overload
    def suggest(self, annotations: Sequence[Annotation]) -> AnnotationList:
        ...

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
    def update(self, item: Annotation | AnnotationUpdate) -> Annotation:
        ...

    @overload
    def update(self, item: Sequence[Annotation | AnnotationUpdate]) -> AnnotationList:
        ...

    def update(
        self, item: Annotation | AnnotationUpdate | Sequence[Annotation | AnnotationUpdate]
    ) -> Annotation | AnnotationList:
        """`Update annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsUpdate>`_

        Args:
            item (Annotation | AnnotationUpdate | Sequence[Annotation | AnnotationUpdate]): Annotation or list of annotations to update (or patch or list of patches to apply)

        Returns:
            Annotation | AnnotationList: No description."""
        return self._update_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, update_cls=AnnotationUpdate, items=item
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
            limit (int | None): Maximum number of results to return. Defaults to None.

        Returns:
            ResourceReferenceList: List of resource references
        """
        assert_type(filter, "filter", types=[AnnotationReverseLookupFilter], allow_none=False)
        assert_type(limit, "limit", [int, type(None)], allow_none=True)

        return self._list(
            list_cls=ResourceReferenceList,
            resource_cls=ResourceReference,
            method="POST",
            limit=limit,
            filter=filter.dump(camel_case=True),
            url_path=self._RESOURCE_PATH + "/reverselookup",
        )

    def list(self, filter: AnnotationFilter | dict, limit: int | None = DEFAULT_LIMIT_READ) -> AnnotationList:
        """`List annotations. <https://developer.cognite.com/api#tag/Annotations/operation/annotationsFilter>`_

        Args:
            filter (AnnotationFilter | dict): Return annotations with parameter values that matches what is specified. Note that annotated_resource_type and annotated_resource_ids are always required.
            limit (int | None): Maximum number of annotations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            AnnotationList: list of annotations
        """
        assert_type(limit, "limit", [int], allow_none=False)
        assert_type(filter, "filter", [AnnotationFilter, dict], allow_none=False)

        if isinstance(filter, AnnotationFilter):
            filter = filter.dump(camel_case=True)

        elif isinstance(filter, dict):
            filter = {to_camel_case(k): v for k, v in filter.items()}

        if "annotatedResourceIds" in filter:
            filter["annotatedResourceIds"] = [
                {to_camel_case(k): v for k, v in f.items()} for f in filter["annotatedResourceIds"]
            ]

        return self._list(list_cls=AnnotationList, resource_cls=Annotation, method="POST", limit=limit, filter=filter)
