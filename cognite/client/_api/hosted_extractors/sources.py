from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.hosted_extractors.sources import Source, SourceList, SourceWrite
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    pass


class SourcesAPI(APIClient):
    _RESOURCE_PATH = "/hostedextractors/sources"

    def create(self, item: SourceWrite | Sequence[SourceWrite]) -> Source | SourceList: ...

    def delete(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False, force: bool = False
    ) -> None: ...

    def retrieve(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> Source: ...

    def update(self, items: SourceWrite | Sequence[SourceWrite]) -> Source | SourceList: ...

    def list(self, limit=DEFAULT_LIMIT_READ) -> SourceList: ...
