import json

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.utils._auxiliary import to_snake_case


class Annotation(CogniteResource):
    def __init__(
        self,
        annotation_type,
        data,
        status,
        creating_app,
        creating_app_version,
        creating_user,
        annotated_resource_type,
        annotated_resource_id=None,
    ):
        self.annotation_type = annotation_type
        self.data = data
        self.status = status
        self.creating_app = creating_app
        self.creating_app_version = creating_app_version
        self.creating_user = creating_user
        self.annotated_resource_type = annotated_resource_type
        self.annotated_resource_id = annotated_resource_id
        self.id: Optional[int] = None
        self.created_time: Optional[int] = None
        self.last_updated_time: Optional[int] = None
        self._cognite_client: CogniteClient = cast("CogniteClient", None)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, dict):
            return cls.from_dict(resource, cognite_client=cognite_client)
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")

    @classmethod
    def from_dict(cls, resource, cognite_client=None):
        data = {to_snake_case(key): val for (key, val) in resource.items()}
        annotation = Annotation(
            annotation_type=data["annotation_type"],
            data=data["data"],
            status=data.get("status", "suggested"),
            creating_app=data["creating_app"],
            creating_app_version=data["creating_app_version"],
            creating_user=data.get("creating_user"),
            annotated_resource_type=data["annotated_resource_type"],
            annotated_resource_id=data.get("annotated_resource_id"),
        )
        annotation.id = data.get("id")
        annotation.created_time = data.get("created_time")
        annotation.last_updated_time = data.get("last_updated_time")
        annotation._cognite_client = cast("CogniteClient", cognite_client)
        return annotation

    def dump(self, camel_case=False):
        result = super().dump(camel_case=camel_case)
        key = "creatingUser" if camel_case else "creating_user"
        result[key] = self.creating_user
        return result


class AnnotationFilter(CogniteFilter):
    def __init__(
        self,
        annotated_resource_type,
        annotated_resource_ids,
        status=None,
        creating_user="",
        creating_app=None,
        creating_app_version=None,
        annotation_type=None,
        data=None,
    ):
        self.annotated_resource_type = annotated_resource_type
        self.annotated_resource_ids = annotated_resource_ids
        self.status = status
        self.creating_user = creating_user
        self.creating_app = creating_app
        self.creating_app_version = creating_app_version
        self.annotation_type = annotation_type
        self.data = data

    def dump(self, camel_case=False):
        result = super().dump(camel_case=camel_case)
        key = "creatingUser" if camel_case else "creating_user"
        if self.creating_user == "":
            del result[key]
        elif self.creating_user is None:
            result[key] = None
        return result


class AnnotationUpdate(CogniteUpdate):
    def __init__(self, id):
        super().__init__(id=id)

    class _StrUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _OptionalStrUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _DictUpdate(CogniteObjectUpdate):
        def set(self, value):
            return self._set(value)

    class _OptionalIntUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    @property
    def data(self):
        return AnnotationUpdate._DictUpdate(self, "data")

    @property
    def status(self):
        return AnnotationUpdate._StrUpdate(self, "status")

    @property
    def annotation_type(self):
        return AnnotationUpdate._StrUpdate(self, "annotationType")


class AnnotationList(CogniteResourceList):
    _RESOURCE = Annotation
