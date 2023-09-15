from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Literal, Union, cast

from typing_extensions import TypeAlias

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    EnumProperty,
    IdTransformerMixin,
)
from cognite.client.data_classes.aggregations import UniqueResult
from cognite.client.data_classes.labels import Label, LabelDefinition
from cognite.client.data_classes.shared import GeoLocation
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SourceFile(CogniteResource):
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
        geo_location (GeoLocation | None): The geolocation of the source file.
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
        geo_location: GeoLocation | None = None,
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
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> SourceFile:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        instance = cls(**convert_all_keys_to_snake_case(resource), cognite_client=cognite_client)
        if isinstance(instance.geo_location, dict):
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.labels:
            output["labels"] = [label.dump(camel_case) for label in self.labels]
        if self.geo_location:
            output[("geoLocation" if camel_case else "geo_location")] = self.geo_location.dump(camel_case)
        for key in ["metadata", "labels", "asset_ids"]:
            # Remove empty lists and dicts:
            if not output[key]:
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
        geo_location (GeoLocation | None): The geolocation of the document.
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
        geo_location: GeoLocation | None = None,
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
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Document:
        resource = json.loads(resource) if isinstance(resource, str) else resource

        instance = cls(**convert_all_keys_to_snake_case(resource), cognite_client=cognite_client)
        if isinstance(instance.source_file, dict):
            instance.source_file = SourceFile._load(instance.source_file)
        if isinstance(instance.geo_location, dict):
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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
class Highlight(CogniteResource):
    """
    Highlighted snippets from name and content fields which show where the query matches are.

    This is used in search results to represent the result.

    Args:
        name (list[str]): Matches in name.
        content (list[str]): Matches in content.
    """

    name: list[str]
    content: list[str]

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "name": self.name,
            "content": self.content,
        }


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
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> DocumentHighlight:
        resource = json.loads(resource) if isinstance(resource, str) else resource

        instance = cls(**convert_all_keys_to_snake_case(resource))
        if isinstance(instance.highlight, dict):
            instance.highlight = Highlight(**convert_all_keys_to_snake_case(instance.highlight))
        if isinstance(instance.document, dict):
            instance.document = Document._load(instance.document)
        return instance

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if self.highlight:
            output["highlight"] = self.highlight.dump(camel_case)
        if self.document:
            output["document"] = self.document.dump(camel_case)
        return output


class DocumentHighlightList(CogniteResourceList[DocumentHighlight]):
    _RESOURCE = DocumentHighlight


class DocumentUniqueResult(UniqueResult):
    ...


class SortableSourceFileProperty(EnumProperty):
    name = "name"
    mime_type = "mimeType"
    source = "source"
    data_set_id = "dataSetId"
    metadata = "metadata"

    def as_reference(self) -> list[str]:
        return ["sourceFile", self.value]


class SourceFileProperty(EnumProperty):
    name = "name"
    mime_type = "mimeType"
    source = "source"
    data_set_id = "dataSetId"
    metadata = "metadata"
    size = "size"
    directory = "directory"
    asset_ids = "assetIds"
    asset_external_ids = "assetExternalIds"
    security_categories = "securityCategories"
    geo_location = "geoLocation"
    labels = "labels"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["sourceFile", "metadata", key]

    def as_reference(self) -> list[str]:
        return ["sourceFile", self.value]


class SortableDocumentProperty(EnumProperty):
    id = "id"
    external_id = "externalId"
    mime_type = "mimeType"
    extension = "extension"
    page_count = "pageCount"
    author = "author"
    title = "title"
    language = "language"
    type = "type"
    created_time = "createdTime"
    modified_time = "modifiedTime"
    last_indexed_time = "lastIndexedTime"


class DocumentProperty(EnumProperty):
    id = "id"
    external_id = "externalId"
    mime_type = "mimeType"
    extension = "extension"
    page_count = "pageCount"
    producer = "producer"
    author = "author"
    title = "title"
    language = "language"
    type = "type"
    created_time = "createdTime"
    modified_time = "modifiedTime"
    last_indexed_time = "lastIndexedTime"
    geo_location = "geoLocation"
    asset_ids = "assetIds"
    asset_external_ids = "assetExternalIds"
    labels = "labels"
    content = "content"


SortableProperty: TypeAlias = Union[SortableSourceFileProperty, SortableDocumentProperty, str, List[str]]


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
