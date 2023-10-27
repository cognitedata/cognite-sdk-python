from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
)
from cognite.client.utils._text import to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Annotation(CogniteResource):
    """Representation of an annotation in CDF.

    Args:
        annotation_type (str): The type of the annotation. This uniquely decides what the structure of the 'data' block will be.
        data (dict): The annotation information. The format of this object is decided by and validated against the 'annotation_type' attribute.
        status (str): The status of the annotation, e.g. "suggested", "approved", "rejected".
        creating_app (str): The name of the app from which this annotation was created.
        creating_app_version (str): The version of the app that created this annotation. Must be a valid semantic versioning (SemVer) string.
        creating_user (str | None): (str, optional): A username, or email, or name. This is not checked nor enforced. If the value is None, it means the annotation was created by a service.
        annotated_resource_type (str): Type name of the CDF resource that is annotated, e.g. "file".
        annotated_resource_id (int | None): The internal ID of the annotated resource.
    """

    def __init__(
        self,
        annotation_type: str,
        data: dict,
        status: str,
        creating_app: str,
        creating_app_version: str,
        creating_user: str | None,
        annotated_resource_type: str,
        annotated_resource_id: int | None = None,
    ) -> None:
        self.annotation_type = annotation_type
        self.data = data
        self.status = status
        self.creating_app = creating_app
        self.creating_app_version = creating_app_version
        self.creating_user = creating_user
        self.annotated_resource_type = annotated_resource_type
        self.annotated_resource_id = annotated_resource_id
        self.id: int | None = None  # Read only
        self.created_time: int | None = None  # Read only
        self.last_updated_time: int | None = None  # Read only
        self._cognite_client: CogniteClient = cast("CogniteClient", None)  # Read only

    @classmethod
    def _load(cls, resource: dict[str, Any] | str, cognite_client: CogniteClient | None = None) -> Annotation:
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, dict):
            return cls.from_dict(resource, cognite_client=cognite_client)
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")

    @classmethod
    def from_dict(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Annotation:
        # Create base annotation
        data = {to_snake_case(key): val for key, val in resource.items()}
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
        # Fill in read-only values, if available
        annotation.id = data.get("id")
        annotation.created_time = data.get("created_time")
        annotation.last_updated_time = data.get("last_updated_time")
        annotation._cognite_client = cast("CogniteClient", cognite_client)
        return annotation

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        # Special handling of created_user, which has a valid None value
        key = "creatingUser" if camel_case else "creating_user"
        result[key] = self.creating_user
        return result


class AnnotationReverseLookupFilter(CogniteFilter):
    """Filter on annotations with various criteria

    Args:
        annotated_resource_type (str): The type of the CDF resource that is annotated, e.g. "file".
        status (str | None): Status of annotations to filter for, e.g. "suggested", "approved", "rejected".
        creating_user (str | None): Name of the user who created the annotations to filter for. Can be set explicitly to "None" to filter for annotations created by a service.
        creating_app (str | None): Name of the app from which the annotations to filter for where created.
        creating_app_version (str | None): Version of the app from which the annotations to filter for were created.
        annotation_type (str | None): Type name of the annotations.
        data (dict[str, Any] | None): The annotation data to filter by. Example format: {"label": "cat", "confidence": 0.9}
    """

    def __init__(
        self,
        annotated_resource_type: str,
        status: str | None = None,
        creating_user: str | None = "",  # None means filtering for a service
        creating_app: str | None = None,
        creating_app_version: str | None = None,
        annotation_type: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.annotated_resource_type = annotated_resource_type
        self.status = status
        self.creating_user = creating_user
        self.creating_app = creating_app
        self.creating_app_version = creating_app_version
        self.annotation_type = annotation_type
        self.data = data

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        # Special handling for creating_user, which has a valid None value
        key = "creatingUser" if camel_case else "creating_user"
        # Remove creating_user if it is an empty string
        if self.creating_user == "":
            del result[key]
        # dump creating_user if it is None
        elif self.creating_user is None:
            result[key] = None
        return result


class AnnotationFilter(AnnotationReverseLookupFilter):
    """Filter on annotations with various criteria

    Args:
        annotated_resource_type (str): The type of the CDF resource that is annotated, e.g. "file".
        annotated_resource_ids (list[dict[str, int]]): List of ids of the annotated CDF resources to filter in. Example format: [{"id": 1234}, {"id": "4567"}]. Must contain at least one item.
        status (str | None): Status of annotations to filter for, e.g. "suggested", "approved", "rejected".
        creating_user (str | None): Name of the user who created the annotations to filter for. Can be set explicitly to "None" to filter for annotations created by a service.
        creating_app (str | None): Name of the app from which the annotations to filter for where created.
        creating_app_version (str | None): Version of the app from which the annotations to filter for were created.
        annotation_type (str | None): Type name of the annotations.
        data (dict[str, Any] | None): The annotation data to filter by. Example format: {"label": "cat", "confidence": 0.9}
    """

    def __init__(
        self,
        annotated_resource_type: str,
        annotated_resource_ids: list[dict[str, int]],
        status: str | None = None,
        creating_user: str | None = "",  # None means filtering for a service
        creating_app: str | None = None,
        creating_app_version: str | None = None,
        annotation_type: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.annotated_resource_ids = annotated_resource_ids
        super().__init__(
            annotated_resource_type=annotated_resource_type,
            status=status,
            creating_user=creating_user,
            creating_app=creating_app,
            creating_app_version=creating_app_version,
            annotation_type=annotation_type,
            data=data,
        )


class AnnotationUpdate(CogniteUpdate):
    """Changes applied to annotation

    Args:
        id (int): A server-generated ID for the object.
    """

    def __init__(self, id: int) -> None:
        super().__init__(id=id)

    class _StrUpdate(CognitePrimitiveUpdate):
        """Only set, no set_null"""

        def set(self, value: str) -> AnnotationUpdate:
            return self._set(value)

    class _OptionalStrUpdate(CognitePrimitiveUpdate):
        """Set and set_null"""

        def set(self, value: str | None) -> AnnotationUpdate:
            return self._set(value)

    class _DictUpdate(CogniteObjectUpdate):
        """Only set, no set_null"""

        def set(self, value: dict[str, Any]) -> AnnotationUpdate:
            return self._set(value)

    class _OptionalIntUpdate(CognitePrimitiveUpdate):
        """Set and set_null"""

        def set(self, value: int | None) -> AnnotationUpdate:
            return self._set(value)

    @property
    def data(self) -> AnnotationUpdate._DictUpdate:
        return AnnotationUpdate._DictUpdate(self, "data")

    @property
    def status(self) -> AnnotationUpdate._StrUpdate:
        return AnnotationUpdate._StrUpdate(self, "status")

    @property
    def annotation_type(self) -> AnnotationUpdate._StrUpdate:
        return AnnotationUpdate._StrUpdate(self, "annotationType")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("data", is_nullable=False),
            PropertySpec("status", is_nullable=False),
            PropertySpec("annotation_type", is_nullable=False),
        ]


class AnnotationList(CogniteResourceList[Annotation]):
    _RESOURCE = Annotation
