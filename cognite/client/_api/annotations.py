from copy import deepcopy
from typing import Any, Dict, List, Sequence, Union

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Annotation, AnnotationFilter, AnnotationList, AnnotationUpdate
from cognite.client.utils._auxiliary import assert_type, to_camel_case
from cognite.client.utils._identifier import IdentifierSequence


class AnnotationsAPI(APIClient):
    _RESOURCE_PATH = "/annotations"

    def create(self, annotations):
        assert_type(annotations, "annotations", [Annotation, Sequence])
        return self._create_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            resource_path=(self._RESOURCE_PATH + "/"),
            items=annotations,
        )

    def suggest(self, annotations):
        assert_type(annotations, "annotations", [Annotation, Sequence])
        items: Union[(List[Dict[(str, Any)]], Dict[(str, Any)])] = (
            [self._sanitize_suggest_item(ann) for ann in annotations]
            if isinstance(annotations, Sequence)
            else self._sanitize_suggest_item(annotations)
        )
        return self._create_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            resource_path=(self._RESOURCE_PATH + "/suggest"),
            items=items,
        )

    @staticmethod
    def _sanitize_suggest_item(annotation):
        item = annotation.dump(camel_case=True) if isinstance(annotation, Annotation) else deepcopy(annotation)
        if "status" in item:
            if item["status"] != "suggested":
                raise ValueError("status field for Annotation suggestions must be set to 'suggested'")
            del item["status"]
        return item

    def list(self, filter, limit=25):
        assert_type(limit, "limit", [int], allow_none=False)
        assert_type(filter, "filter", [AnnotationFilter, dict], allow_none=False)
        if isinstance(filter, AnnotationFilter):
            filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            filter = {to_camel_case(k): v for (k, v) in filter.items()}
        if "annotatedResourceIds" in filter:
            filter["annotatedResourceIds"] = [
                {to_camel_case(k): v for (k, v) in f.items()} for f in filter["annotatedResourceIds"]
            ]
        return self._list(list_cls=AnnotationList, resource_cls=Annotation, method="POST", limit=limit, filter=filter)

    @staticmethod
    def _convert_resource_to_patch_object(resource, update_attributes):
        if not isinstance(resource, Annotation):
            return APIClient._convert_resource_to_patch_object(resource, update_attributes)
        annotation: Annotation = resource
        assert annotation.id is not None
        annotation_update = AnnotationUpdate(id=annotation.id)
        for attr in update_attributes:
            getattr(annotation_update, attr).set(getattr(annotation, attr))
        return annotation_update.dump()

    def update(self, item):
        return self._update_multiple(
            list_cls=AnnotationList, resource_cls=Annotation, update_cls=AnnotationUpdate, items=item
        )

    def delete(self, id):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)

    def retrieve_multiple(self, ids):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=None)
        return self._retrieve_multiple(list_cls=AnnotationList, resource_cls=Annotation, identifiers=identifiers)

    def retrieve(self, id):
        identifiers = IdentifierSequence.load(ids=id, external_ids=None).as_singleton()
        return self._retrieve_multiple(list_cls=AnnotationList, resource_cls=Annotation, identifiers=identifiers)
