from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.three_d.asset_mapping import ThreeDAssetMappingAPI
from cognite.client._api.three_d.files import ThreeDFilesAPI
from cognite.client._api.three_d.models import ThreeDModelsAPI
from cognite.client._api.three_d.revisions import ThreeDRevisionsAPI
from cognite.client._api_client import APIClient

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class ThreeDAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.models = ThreeDModelsAPI(config, api_version, cognite_client)
        self.revisions = ThreeDRevisionsAPI(config, api_version, cognite_client)
        self.files = ThreeDFilesAPI(config, api_version, cognite_client)
        self.asset_mappings = ThreeDAssetMappingAPI(config, api_version, cognite_client)
