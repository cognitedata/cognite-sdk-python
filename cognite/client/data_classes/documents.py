from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from cognite.client.data_classes import GeoLocation, Label, LabelDefinition
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SourceFile(CogniteResource):
    def __init__(
        self,
        name: str,
        hash: Optional[str] = None,
        directory: Optional[str] = None,
        source: Optional[str] = None,
        mime_type: Optional[str] = None,
        size: Optional[int] = None,
        asset_ids: Optional[list[int]] = None,
        labels: Optional[list[Label | str | LabelDefinition]] = None,
        geo_location: Optional[GeoLocation] = None,
        dataset_id: Optional[int] = None,
        security_categories: Optional[list[int]] = None,
        metadata: Optional[dict[str, str]] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
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
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> SourceFile:
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
        return output


class Document(CogniteResource):
    def __init__(
        self,
        id: int,
        created_time: int,
        source_file: SourceFile,
        external_id: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
        modified_time: Optional[int] = None,
        last_indexed_time: Optional[int] = None,
        mime_type: Optional[str] = None,
        extension: Optional[str] = None,
        page_count: Optional[int] = None,
        type: Optional[str] = None,
        language: Optional[str] = None,
        truncated_content: Optional[str] = None,
        asset_ids: Optional[list[int]] = None,
        labels: Optional[list[Label | str | LabelDefinition]] = None,
        geo_location: Optional[GeoLocation] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
        self.id = id
        self.created_time = created_time
        self.source_file = source_file
        self.external_id = external_id
        self.title = title
        self.author = author
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
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> Document:
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


@dataclass
class Highlight(CogniteResource):
    name: list[str]
    content: list[str]

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "name": self.name,
            "content": self.content,
        }


@dataclass
class DocumentHighlight(CogniteResource):
    highlight: Highlight
    document: Document

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> DocumentHighlight:
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


class DocumentList(CogniteResourceList[Document]):
    _RESOURCE = Document


class DocumentHighlightList(CogniteResourceList[DocumentHighlight]):
    _RESOURCE = DocumentHighlight
