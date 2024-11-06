from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._text import to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient

AnnotationType: TypeAlias = Literal[
    "images.ObjectDetection",
    "images.Classification",
    "images.KeypointCollection",
    "images.AssetLink",
    "images.TextRegion",
    "images.InstanceLink",
    "isoplan.IsoPlanAnnotation",
    "diagrams.AssetLink",
    "diagrams.FileLink",
    "diagrams.InstanceLink",
    "diagrams.UnhandledTextObject",
    "diagrams.UnhandledSymbolObject",
    "documents.ExtractedText",
    "diagrams.Line",
    "diagrams.Junction",
    "pointcloud.BoundingVolume",
    "forms.Detection",
]


class AnnotationCore(WriteableCogniteResource["AnnotationWrite"], ABC):
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        # Special handling of created_user, which has a valid None value
        key = "creatingUser" if camel_case else "creating_user"
        result[key] = self.creating_user
        return result


class Annotation(AnnotationCore):
    """Representation of an annotation in CDF.
    This is the reading version of the Annotation class. It is never to be used when creating new annotations.

    Args:
        annotation_type (str): The type of the annotation. This uniquely decides what the structure of the 'data' block will be.
        data (dict): The annotation information. The format of this object is decided by and validated against the 'annotation_type' attribute.
        status (str): The status of the annotation, e.g. "suggested", "approved", "rejected".
        creating_app (str): The name of the app from which this annotation was created.
        creating_app_version (str): The version of the app that created this annotation. Must be a valid semantic versioning (SemVer) string.
        creating_user (str | None): (str, optional): A username, or email, or name. This is not checked nor enforced. If the value is None, it means the annotation was created by a service.
        annotated_resource_type (str): Type name of the CDF resource that is annotated, e.g. "file".
        annotated_resource_id (int | None): The internal ID of the annotated resource.
        id (int | None): A server-generated ID for the object.
        created_time (int | None): The timestamp for when the annotation was created, in milliseconds since epoch.
        last_updated_time (int | None): The timestamp for when the annotation was last updated, in milliseconds since epoch.
        cognite_client (CogniteClient | None): The client to associate with this object.
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
        id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            annotation_type,
            data,
            status,
            creating_app,
            creating_app_version,
            creating_user,
            annotated_resource_type,
            annotated_resource_id,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Annotation:
        return cls.from_dict(resource, cognite_client=cognite_client)

    @classmethod
    def from_dict(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Annotation:
        # Create base annotation
        data = {to_snake_case(key): val for key, val in resource.items()}
        return Annotation(
            annotation_type=data["annotation_type"],
            data=data["data"],
            status=data.get("status", "suggested"),
            creating_app=data["creating_app"],
            creating_app_version=data["creating_app_version"],
            creating_user=data.get("creating_user"),
            annotated_resource_type=data["annotated_resource_type"],
            annotated_resource_id=data.get("annotated_resource_id"),
            id=data.get("id"),
            created_time=data.get("created_time"),
            last_updated_time=data.get("last_updated_time"),
            cognite_client=cognite_client,
        )

    def as_write(self) -> AnnotationWrite:
        """Returns this Annotation in its writing version."""
        if self.annotated_resource_id is None:
            raise ValueError("Annotated resource ID is required for the writing version of an annotation.")
        return AnnotationWrite(
            annotation_type=cast(AnnotationType, self.annotation_type),
            data=self.data,
            status=cast(Literal["suggested", "approved", "rejected"], self.status),
            creating_app=self.creating_app,
            creating_app_version=self.creating_app_version,
            creating_user=self.creating_user,
            annotated_resource_type=cast(Literal["file", "threedmodel"], self.annotated_resource_type),
            annotated_resource_id=self.annotated_resource_id,
        )


class AnnotationWrite(AnnotationCore):
    """Representation of an annotation in CDF.
    This is the writing version of the Annotation class. It is used when creating new annotations.

    Args:
        annotation_type (AnnotationType): The type of the annotation. This uniquely decides what the structure of the 'data' block will be.
        data (dict): The annotation information. The format of this object is decided by and validated against the 'annotation_type' attribute.
        status (Literal['suggested', 'approved', 'rejected']): The status of the annotation, e.g. "suggested", "approved", "rejected".
        creating_app (str): The name of the app from which this annotation was created.
        creating_app_version (str): The version of the app that created this annotation. Must be a valid semantic versioning (SemVer) string.
        creating_user (str | None): A username, or email, or name. This is not checked nor enforced. If the value is None, it means the annotation was created by a service.
        annotated_resource_type (Literal['file', 'threedmodel']): Type name of the CDF resource that is annotated, e.g. "file".
        annotated_resource_id (int): The internal ID of the annotated resource.
    """

    def __init__(
        self,
        annotation_type: AnnotationType,
        data: dict,
        status: Literal["suggested", "approved", "rejected"],
        creating_app: str,
        creating_app_version: str,
        creating_user: str | None,
        annotated_resource_type: Literal["file", "threedmodel"],
        annotated_resource_id: int,
    ) -> None:
        super().__init__(
            annotation_type=annotation_type,
            data=data,
            status=status,
            creating_app=creating_app,
            creating_app_version=creating_app_version,
            creating_user=creating_user,
            annotated_resource_type=annotated_resource_type,
            annotated_resource_id=annotated_resource_id,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AnnotationWrite:
        return cls(
            annotation_type=resource["annotationType"],
            data=resource["data"],
            status=resource["status"],
            creating_app=resource["creatingApp"],
            creating_app_version=resource["creatingAppVersion"],
            creating_user=resource["creatingUser"],
            annotated_resource_type=resource["annotatedResourceType"],
            annotated_resource_id=resource["annotatedResourceId"],
        )

    def as_write(self) -> AnnotationWrite:
        """Returns this AnnotationWrite."""
        return self


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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
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
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("data", is_nullable=False),
            PropertySpec("status", is_nullable=False),
            PropertySpec("annotation_type", is_nullable=False),
        ]


class AnnotationWriteList(CogniteResourceList[AnnotationWrite]):
    _RESOURCE = AnnotationWrite


class AnnotationList(WriteableCogniteResourceList[AnnotationWrite, Annotation]):
    _RESOURCE = Annotation

    def as_write(self) -> AnnotationWriteList:
        """Returns this AnnotationList in its writing version."""
        return AnnotationWriteList([ann.as_write() for ann in self.data], cognite_client=self._get_cognite_client())
