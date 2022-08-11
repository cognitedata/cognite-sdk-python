from copy import deepcopy
from typing import Any, Collection, Dict, List, Optional, Union, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._auxiliary import assert_type, to_camel_case
from cognite.client.utils._identifier import IdentifierSequence


class AnnotationsAPI(APIClient):
    _RESOURCE_PATH = "/annotations"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    @overload
    def create(self, annotations: Annotation) -> Annotation:
        ...

    @overload
    def create(self, annotations: List[Annotation]) -> AnnotationList:
        ...

    def create(self, annotations: Union[Annotation, List[Annotation]]) -> Union[Annotation, AnnotationList]:
        """Create annotations

        Args:
            annotations (Union[Annotation, List[Annotation]]): annotation(s) to create

        Returns:
            Union[Annotation, AnnotationList]: created annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, list])
        return self._create_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, resource_path=self._RESOURCE_PATH + "/", items=annotations
        )

    @overload
    def suggest(self, annotations: Annotation) -> Annotation:
        ...

    @overload
    def suggest(self, annotations: List[Annotation]) -> AnnotationList:
        ...

    def suggest(self, annotations: Union[Annotation, List[Annotation]]) -> Union[Annotation, AnnotationList]:
        """Suggest annotations

        Args:
            annotations (Union[Annotation, List[Annotation]]): annotation(s) to suggest. They must have status set to "suggested".

        Returns:
            Union[Annotation, AnnotationList]: suggested annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, list])
        # Deal with status fields in both cases: Single item and list of items
        items: Union[List[Dict[str, Any]], Dict[str, Any]] = (
            [self._sanitize_suggest_item(ann) for ann in annotations]
            if isinstance(annotations, list)
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
            filter (AnnotationFilter, optional): Return annotations with parameter values that matches what is specified. Note that annotated_resource_type and annotated_resource_ids are always required.

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
    def update(self, item: List[Union[Annotation, AnnotationUpdate]]) -> AnnotationList:
        ...

    def update(
        self, item: Union[Annotation, AnnotationUpdate, List[Union[Annotation, AnnotationUpdate]]]
    ) -> Union[Annotation, AnnotationList]:
        """Update annotations

        Args:
            id (Union[int, List[int]]): ID or list of IDs to be deleted
        """
        return self._update_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, update_cls=AnnotationUpdate, items=item
        )

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete annotations

        Args:
            id (Union[int, List[int]]): ID or list of IDs to be deleted
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)

    def retrieve_multiple(self, ids: List[int]) -> AnnotationList:
        """Retrieve annotations by IDs

        Args:
            ids (List[int]]: list of IDs to be retrieved

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
