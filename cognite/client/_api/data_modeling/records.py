from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.records import (
    LastUpdatedRange,
    RecordId,
    RecordIngest,
    RecordList,
    RecordListWithCursor,
)
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._experimental import FeaturePreviewWarning, warn_on_all_method_invocations

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


@warn_on_all_method_invocations(
    FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Records API")
)
class RecordsAPI(APIClient):
    _RESOURCE_PATH = "/streams/{}/records"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.__alpha_headers = {
            "cdf-version": "alpha",
        }

    def ingest(self, stream: str, records: Sequence[RecordIngest]) -> None:
        body = {"items": [record.dump(camel_case=True) for record in records]}
        self._post(url_path=self._RESOURCE_PATH.format(stream), json=body, headers=self.__alpha_headers)

    def upsert(self, stream: str, records: Sequence[RecordIngest]) -> None:
        body = {"items": [record.dump(camel_case=True) for record in records]}
        self._post(url_path=self._RESOURCE_PATH.format(stream) + "/upsert", json=body, headers=self.__alpha_headers)

    def delete(self, stream: str, ids: RecordId | Sequence[RecordId]) -> None:
        items = ids if isinstance(ids, Sequence) else [ids]
        body = {"items": [item.dump(camel_case=True) for item in items]}
        self._post(url_path=self._RESOURCE_PATH.format(stream) + "/delete", json=body, headers=self.__alpha_headers)

    def filter(
        self,
        stream: str,
        *,
        last_updated_time: LastUpdatedRange | None = None,
        filter: Filter | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> RecordList:
        body: dict[str, Any] = {}
        if last_updated_time is not None:
            body["lastUpdatedTime"] = last_updated_time.dump()
        if filter is not None:
            body["filter"] = filter.dump()
        body["limit"] = limit
        res = self._post(
            url_path=self._RESOURCE_PATH.format(stream) + "/filter", json=body, headers=self.__alpha_headers
        )
        return RecordList._load(res.json()["items"], cognite_client=self._cognite_client)

    def sync(
        self,
        stream: str,
        *,
        filter: Filter | None = None,
        cursor: str | None = None,
        initialize_cursor: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> RecordListWithCursor:
        body: dict = {}
        if filter is not None:
            body["filter"] = filter.dump()
        if cursor is not None:
            body["cursor"] = cursor
        if initialize_cursor is not None:
            body["initializeCursor"] = initialize_cursor
        body["limit"] = limit
        res = self._post(url_path=self._RESOURCE_PATH.format(stream) + "/sync", json=body, headers=self.__alpha_headers)
        payload = res.json()
        items = RecordList._load(payload["items"], cognite_client=self._cognite_client)
        return RecordListWithCursor(list(items), cursor=payload.get("nextCursor"), has_next=payload.get("hasNext"))
