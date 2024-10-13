from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from enum import auto
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    EnumProperty,
    Geometry,
    IdTransformerMixin,
)
from cognite.client.data_classes.aggregations import UniqueResult
from cognite.client.data_classes.labels import Label, LabelDefinition

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class DocumentsGeoJsonGeometry(CogniteObject):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (Literal['Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon', 'GeometryCollection']): The geometry type.
        coordinates (list | None): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
        geometries (Collection[Geometry] | None): No description.

    Examples:
        Point:
            Coordinates of a point in 2D space, described as an array of 2 numbers.

            Example: `[4.306640625, 60.205710352530346]`

        LineString:
            Coordinates of a line described by a list of two or more points.
            Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

            Example: `[[30, 10], [10, 30], [40, 40]]`

        Polygon:
            List of one or more linear rings representing a shape.
            A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.
            Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

            Example: `[[[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]], [[20, 30], [35, 35], [30, 20], [20, 30]]]`
            type: array

        MultiPoint:
            List of Points. Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

            Example: `[[35, 10], [45, 45]]`

        MultiLineString:
                List of lines where each line (LineString) is defined as a list of two or more points.
                Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

                Example: `[[[30, 10], [10, 30]], [[35, 10], [10, 30], [40, 40]]]`

        MultiPolygon:
            List of multiple polygons.
            Each polygon is defined as a list of one or more linear rings representing a shape.
            A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.
            Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

            Example: `[[[[30, 20], [45, 40], [10, 40], [30, 20]]], [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]]`

        GeometryCollection:
            List of geometries as described above.
    """

    _VALID_TYPES = frozenset(
        {"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection"}
    )

    def __init__(
        self,
        type: Literal[
            "Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection"
        ],
        coordinates: list | None = None,
        geometries: Collection[Geometry] | None = None,
    ) -> None:
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates
        self.geometries = geometries and list(geometries)

    @classmethod
    def _load(
        cls, raw_geometry: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> DocumentsGeoJsonGeometry:
        instance = cls(
            type=raw_geometry["type"],
            coordinates=raw_geometry.get("coordinates"),
            geometries=raw_geometry.get("geometries"),
        )
        if isinstance(instance.geometries, list):
            instance.geometries = [Geometry.load(geometry) for geometry in instance.geometries]
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.geometries:
            output["geometries"] = [g.dump(camel_case) for g in self.geometries]
        return output


class SourceFile(CogniteObject):
    """
    The source file that a document is derived from.

    Args:
        name (str): The name of the source file.
        hash (str | None): The hash of the source file. This is a SHA256 hash of the original file. The hash only covers the file content, and not other CDF metadata.
        directory (str | None): The directory the file can be found in.
        source (str | None): The source of the file.
        mime_type (str | None): The mime type of the file.
        size (int | None): The size of the file in bytes.
        asset_ids (list[int] | None): The ids of the assets related to this file.
        labels (list[Label | str | LabelDefinition] | None): A list of labels associated with this document's source file in CDF.
        geo_location (DocumentsGeoJsonGeometry | None): The geolocation of the source file.
        dataset_id (int | None): The id if the dataset this file belongs to, if any.
        security_categories (list[int] | None): The security category IDs required to access this file.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value.
        cognite_client (CogniteClient | None): No description.
        **_ (Any): No description.
    """

    def __init__(
        self,
        name: str,
        hash: str | None = None,
        directory: str | None = None,
        source: str | None = None,
        mime_type: str | None = None,
        size: int | None = None,
        asset_ids: list[int] | None = None,
        labels: list[Label | str | LabelDefinition] | None = None,
        geo_location: DocumentsGeoJsonGeometry | None = None,
        dataset_id: int | None = None,
        security_categories: list[int] | None = None,
        metadata: dict[str, str] | None = None,
        cognite_client: CogniteClient | None = None,
        **_: Any,
    ) -> None:
        self.name = name
        self.hash = hash
        self.directory = directory
        self.source = source
        self.mime_type = mime_type
        self.size = size
        self.asset_ids: list[int] = asset_ids or []
        self.labels: list[Label] = Label._load_list(labels) or []
        self.geo_location = geo_location
        self.dataset_id = dataset_id
        self.security_categories = security_categories
        self.metadata: dict[str, str] = metadata or {}
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SourceFile:
        return cls(
            name=resource["name"],
            hash=resource.get("hash"),
            directory=resource.get("directory"),
            source=resource.get("source"),
            mime_type=resource.get("mimeType"),
            size=resource.get("size"),
            asset_ids=resource.get("assetIds"),
            labels=Label._load_list(resource.get("labels")),  # type: ignore[arg-type]
            geo_location=DocumentsGeoJsonGeometry._load(resource["geoLocation"]) if "geoLocation" in resource else None,
            dataset_id=resource.get("datasetId"),
            security_categories=resource.get("securityCategories"),
            metadata=resource.get("metadata"),
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.labels:
            output["labels"] = [label.dump(camel_case) for label in self.labels]
        if self.geo_location:
            output[("geoLocation" if camel_case else "geo_location")] = self.geo_location.dump(camel_case)
        for key in ["metadata", "labels", "asset_ids", "assetIds"]:
            # Remove empty lists and dicts
            if key in output and not output[key]:
                del output[key]
        return output


class Document(CogniteResource):
    """
    A representation of a document in CDF.

    Args:
        id (int): A server-generated ID for the object.
        created_time (int): The creation time of the document in CDF in milliseconds since Jan 1, 1970.
        source_file (SourceFile): The source file that this document is derived from.
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        title (str | None): The title of the document.
        author (str | None): The author of the document.
        producer (str | None): The producer of the document. Many document types contain metadata indicating what software or system was used to create the document.
        modified_time (int | None): The last time the document was modified in CDF in milliseconds since Jan 1, 1970.
        last_indexed_time (int | None): The last time the document was indexed in the search engine, measured in milliseconds since Jan 1, 1970.
        mime_type (str | None): The detected mime type of the document.
        extension (str | None): Extension of the file (always in lowercase)
        page_count (int | None): The number of pages in the document.
        type (str | None): The detected type of the document.
        language (str | None): The detected language of the document.
        truncated_content (str | None): The truncated content of the document.
        asset_ids (list[int] | None): The ids of any assets referred to in the document.
        labels (list[Label | str | LabelDefinition] | None): The labels attached to the document.
        geo_location (DocumentsGeoJsonGeometry | None): The geolocation of the document.
        cognite_client (CogniteClient | None): No description.
        **_ (Any): No description.
    """

    def __init__(
        self,
        id: int,
        created_time: int,
        source_file: SourceFile,
        external_id: str | None = None,
        title: str | None = None,
        author: str | None = None,
        producer: str | None = None,
        modified_time: int | None = None,
        last_indexed_time: int | None = None,
        mime_type: str | None = None,
        extension: str | None = None,
        page_count: int | None = None,
        type: str | None = None,
        language: str | None = None,
        truncated_content: str | None = None,
        asset_ids: list[int] | None = None,
        labels: list[Label | str | LabelDefinition] | None = None,
        geo_location: DocumentsGeoJsonGeometry | None = None,
        cognite_client: CogniteClient | None = None,
        **_: Any,
    ) -> None:
        self.id = id
        self.created_time = created_time
        self.source_file = source_file
        self.external_id = external_id
        self.title = title
        self.author = author
        self.producer = producer
        self.modified_time = modified_time
        self.last_indexed_time = last_indexed_time
        self.mime_type = mime_type
        self.extension = extension
        self.page_count = page_count
        self.type = type
        self.language = language
        self.truncated_content = truncated_content
        self.asset_ids: list[int] = asset_ids or []
        self.labels: list[Label] = Label._load_list(labels) or []
        self.geo_location = geo_location
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Document:
        return cls(
            id=resource["id"],
            created_time=resource["createdTime"],
            source_file=SourceFile._load(resource["sourceFile"]),
            external_id=resource.get("externalId"),
            title=resource.get("title"),
            author=resource.get("author"),
            producer=resource.get("producer"),
            modified_time=resource.get("modifiedTime"),
            last_indexed_time=resource.get("lastIndexedTime"),
            mime_type=resource.get("mimeType"),
            extension=resource.get("extension"),
            page_count=resource.get("pageCount"),
            type=resource.get("type"),
            language=resource.get("language"),
            truncated_content=resource.get("truncatedContent"),
            asset_ids=resource.get("assetIds"),
            labels=Label._load_list(resource.get("labels")),  # type: ignore[arg-type]
            geo_location=DocumentsGeoJsonGeometry._load(resource["geoLocation"]) if "geoLocation" in resource else None,
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.source_file:
            output[("sourceFile" if camel_case else "source_file")] = self.source_file.dump(camel_case)
        if self.labels:
            output["labels"] = [label.dump(camel_case) for label in self.labels]
        if self.geo_location:
            output[("geoLocation" if camel_case else "geo_location")] = self.geo_location.dump(camel_case)
        return output


class DocumentList(CogniteResourceList[Document], IdTransformerMixin):
    _RESOURCE = Document


@dataclass
class Highlight(CogniteObject):
    """
    Highlighted snippets from name and content fields which show where the query matches are.

    This is used in search results to represent the result.

    Args:
        name (list[str]): Matches in name.
        content (list[str]): Matches in content.
    """

    name: list[str]
    content: list[str]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "name": self.name,
            "content": self.content,
        }

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(name=resource["name"], content=resource["content"])


@dataclass
class DocumentHighlight(CogniteResource):
    """
    A pair of a document and highlights.

    This is used in search results to represent the result

    Args:
        highlight (Highlight): The highlight from the document matching search results.
        document (Document): The document.
    """

    highlight: Highlight
    document: Document

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> DocumentHighlight:
        return cls(
            highlight=Highlight._load(resource["highlight"]),
            document=Document._load(resource["document"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if self.highlight:
            output["highlight"] = self.highlight.dump(camel_case)
        if self.document:
            output["document"] = self.document.dump(camel_case)
        return output


class DocumentHighlightList(CogniteResourceList[DocumentHighlight]):
    _RESOURCE = DocumentHighlight


class DocumentUniqueResult(UniqueResult): ...


class SortableSourceFileProperty(EnumProperty):
    name = auto()  # type: ignore [assignment]
    mime_type = auto()
    source = auto()
    data_set_id = auto()
    metadata = auto()

    def as_reference(self) -> list[str]:
        return ["sourceFile", self.value]


class SourceFileProperty(EnumProperty):
    name = auto()  # type: ignore [assignment]
    mime_type = auto()
    source = auto()
    data_set_id = auto()
    metadata = auto()
    size = auto()
    directory = auto()
    asset_ids = auto()
    asset_external_ids = auto()
    security_categories = auto()
    geo_location = auto()
    labels = auto()

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["sourceFile", "metadata", key]

    def as_reference(self) -> list[str]:
        return ["sourceFile", self.value]


class SortableDocumentProperty(EnumProperty):
    id = auto()
    external_id = auto()
    mime_type = auto()
    extension = auto()
    page_count = auto()
    author = auto()
    title = auto()
    language = auto()
    type = auto()
    created_time = auto()
    modified_time = auto()
    last_indexed_time = auto()


class DocumentProperty(EnumProperty):
    id = auto()
    external_id = auto()
    mime_type = auto()
    extension = auto()
    page_count = auto()
    producer = auto()
    author = auto()
    title = auto()
    language = auto()
    type = auto()
    created_time = auto()
    modified_time = auto()
    last_indexed_time = auto()
    geo_location = auto()
    asset_ids = auto()
    asset_external_ids = auto()
    labels = auto()
    content = auto()


SortableProperty: TypeAlias = SortableSourceFileProperty | SortableDocumentProperty | str | list[str]


class DocumentSort(CogniteSort):
    def __init__(self, property: SortableProperty, order: Literal["asc", "desc"] = "asc"):
        super().__init__(property, order)


@dataclass
class TemporaryLink:
    url: str
    expires_at: int

    @classmethod
    def load(cls, data: dict[str, Any]) -> TemporaryLink:
        return cls(
            url=data["temporaryLink"],
            expires_at=data["expirationTime"],
        )

    def dump(self) -> dict[str, Any]:
        return {
            "temporaryLink": self.url,
            "expirationTime": self.expires_at,
        }
