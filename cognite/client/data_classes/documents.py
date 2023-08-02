from __future__ import annotations

from typing import Optional

from cognite.client.data_classes import GeoLocation, Label, LabelDefinition
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class SourceFile(CogniteResource):
    def __init__(
        self,
        name: str,
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
    ):
        self.name = name
        self.directory = directory
        self.source = source
        self.mime_type = mime_type
        self.size = size
        self.asset_ids: list[int] = asset_ids or []
        self.labels = labels
        self.geo_location = geo_location
        self.dataset_id = dataset_id
        self.security_categories = security_categories
        self.metadata: dict[str, str] = metadata or {}


class Document(CogniteResource):
    def __init__(
        self,
        id: str,
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
        self.labels = labels
        self.geo_location = geo_location


class DocumentList(CogniteResourceList[Document]):
    _RESOURCE = Document
