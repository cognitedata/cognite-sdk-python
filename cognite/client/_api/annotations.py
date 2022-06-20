from copy import deepcopy
from typing import Dict, List, Union

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._auxiliary import assert_type, to_camel_case


class AnnotationsAPI(APIClient):
    _RESOURCE_PATH = "/annotations"
    _LIST_CLASS = AnnotationList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create(self, annotations: Union[Annotation, List[Annotation]]) -> Union[Annotation, AnnotationList]:
        """Create annotations

        Args:
            annotations (Union[Annotation, List[Annotation]]): annotation(s) to create

        Returns:
            Union[Annotation, AnnotationList]: created annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, list])
        return self._create_multiple(resource_path=self._RESOURCE_PATH + "/", items=annotations)

    def suggest(self, annotations: Union[Annotation, List[Annotation]]) -> Union[Annotation, AnnotationList]:
        """Suggest annotations

        Args:
            annotations (Union[Annotation, List[Annotation]]): annotation(s) to suggest. They must have status set to "suggested".

        Returns:
            Union[Annotation, AnnotationList]: suggested annotation(s)
        """
        assert_type(annotations, "annotations", [Annotation, list])
        # Deal with status fields in both cases: Single item and list of items
        items = (
            [AnnotationsAPI._sanitize_suggest_item(ann) for ann in annotations]
            if isinstance(annotations, list)
            else AnnotationsAPI._sanitize_suggest_item(annotations)
        )
        return self._create_multiple(resource_path=self._RESOURCE_PATH + "/suggest", items=items)

    @staticmethod
    def _sanitize_suggest_item(annotation: Annotation) -> Dict[str, any]:
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

        return self._list(method="POST", limit=limit, filter=filter)

    @staticmethod
    def _convert_resource_to_patch_object(resource: CogniteResource, update_attributes: List[str]):
        if not isinstance(resource, Annotation):
            return APIClient._convert_resource_to_patch_object(resource, update_attributes)
        annotation: Annotation = resource
        annotation_update = AnnotationUpdate(id=annotation.id)
        for attr in update_attributes:
            getattr(annotation_update, attr).set(getattr(annotation, attr))
        return annotation_update.dump()

    def update(
        self, item: Union[Annotation, AnnotationUpdate, List[Union[Annotation, AnnotationUpdate]]]
    ) -> Union[Annotation, AnnotationList]:
        """Update annotations

        Args:
            id (Union[int, List[int]]): ID or list of IDs to be deleted
        """
        return self._update_multiple(items=item)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete annotations

        Args:
            id (Union[int, List[int]]): ID or list of IDs to be deleted
        """
        self._delete_multiple(ids=id, wrap_ids=True)

    def retrieve_multiple(self, ids: List[int]) -> AnnotationList:
        """Retrieve annotations by IDs

        Args:
            ids (List[int]]: list of IDs to be retrieved

        Returns:
            AnnotationList: list of annotations
        """
        assert_type(ids, "ids", [List], allow_none=False)
        return self._retrieve_multiple(ids=ids, wrap_ids=True)

    def retrieve(self, id: int) -> Annotation:
        """Retrieve an annotation by id

        Args:
            id (int): id of the annotation to be retrieved

        Returns:
            Annotation: annotation requested
        """
        assert_type(id, "id", [int], allow_none=False)
        return self._retrieve_multiple(ids=id, wrap_ids=True)
