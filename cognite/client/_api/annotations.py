from __future__ import annotations

from copy import deepcopy
from typing import Any, Collection, Dict, List, Optional, Sequence, Union, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._auxiliary import assert_type, to_camel_case
from cognite.client.utils._identifier import IdentifierSequence


class AnnotationsAPI(APIClient):
    _RESOURCE_PATH = "/annotations"

    @overload
    def create(self, annotations: Annotation) -> Annotation:
        ...

    @overload
    def create(self, annotations: Sequence[Annotation]) -> AnnotationList:
        ...

    def create(self, annotations: Union[Annotation, Sequence[Annotation]]) -> Union[Annotation, AnnotationList]:
        """Create annotations

        Args:
            annotations (Union[Annotation, Sequence[Annotation]]): annotation(s) to create

        Returns:
            Union[Annotation, AnnotationList]: created annotation(s)
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

    def suggest(self, annotations: Union[Annotation, Sequence[Annotation]]) -> Union[Annotation, AnnotationList]:
        """Suggest annotations

        Args:
            annotations (Union[Annotation, Sequence[Annotation]]): annotation(s) to suggest. They must have status set to "suggested".

        Returns:
            Union[Annotation, AnnotationList]: suggested annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, Sequence])
        # Deal with status fields in both cases: Single item and list of items
        items: Union[List[Dict[str, Any]], Dict[str, Any]] = (
            [self._sanitize_suggest_item(ann) for ann in annotations]
            if isinstance(annotations, Sequence)
            else self._sanitize_suggest_item(annotations)
        )
        return self._create_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            resource_path=self._RESOURCE_PATH + "/suggest",
            items=items,  # type: ignore[arg-type]
        )

    @staticmethod
    def _sanitize_suggest_item(annotation: Union[Annotation, Dict[str, Any]]) -> Dict[str, Any]:
        # Check that status is set to suggested if it is set and afterwards remove it
        item = annotation.dump(camel_case=True) if isinstance(annotation, Annotation) else deepcopy(annotation)
        if "status" in item:
            if item["status"] != "suggested":
                raise ValueError("status field for Annotation suggestions must be set to 'suggested'")
            del item["status"]
        return item

    def list(self, filter: Union[AnnotationFilter, Dict], limit: int = 25) -> AnnotationList:
        """List annotations.

        Args:
            limit (int): Maximum number of annotations to return. Defaults to 25.
            filter (AnnotationFilter): Return annotations with parameter values that matches what is specified. Note that annotated_resource_type and annotated_resource_ids are always required.

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

    @staticmethod
    def _convert_resource_to_patch_object(
        resource: CogniteResource, update_attributes: Collection[str]
    ) -> Dict[str, Dict[str, Dict]]:
        if not isinstance(resource, Annotation):
            return APIClient._convert_resource_to_patch_object(resource, update_attributes)
        annotation: Annotation = resource

        assert annotation.id is not None
        annotation_update = AnnotationUpdate(id=annotation.id)
        for attr in update_attributes:
            getattr(annotation_update, attr).set(getattr(annotation, attr))
        return annotation_update.dump()

    @overload
    def update(self, item: Union[Annotation, AnnotationUpdate]) -> Annotation:
        ...

    @overload
    def update(self, item: Sequence[Union[Annotation, AnnotationUpdate]]) -> AnnotationList:
        ...

    def update(
        self, item: Union[Annotation, AnnotationUpdate, Sequence[Union[Annotation, AnnotationUpdate]]]
    ) -> Union[Annotation, AnnotationList]:
        """Update annotations

        Args:
            id (Union[int, Sequence[int]]): ID or list of IDs to be deleted
        """
        return self._update_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, update_cls=AnnotationUpdate, items=item
        )

    def delete(self, id: Union[int, Sequence[int]]) -> None:
        """Delete annotations

        Args:
            id (Union[int, Sequence[int]]): ID or list of IDs to be deleted
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)

    def retrieve_multiple(self, ids: Sequence[int]) -> AnnotationList:
        """Retrieve annotations by IDs

        Args:
            ids (Sequence[int]]: list of IDs to be retrieved

        Returns:
            AnnotationList: list of annotations
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=None)
        return self._retrieve_multiple(list_cls=AnnotationList, resource_cls=Annotation, identifiers=identifiers)

    def retrieve(self, id: int) -> Optional[Annotation]:
        """Retrieve an annotation by id

        Args:
            id (int): id of the annotation to be retrieved

        Returns:
            Annotation: annotation requested
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None).as_singleton()
        return self._retrieve_multiple(list_cls=AnnotationList, resource_cls=Annotation, identifiers=identifiers)
