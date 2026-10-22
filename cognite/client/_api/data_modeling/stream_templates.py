from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.streams import StreamTemplateInfo, StreamTemplateInfoList
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class StreamTemplatesAPI(APIClient):
    _RESOURCE_PATH = "/streams/templates"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="alpha", sdk_maturity="alpha", feature_name="Stream templates"
        )

    async def list(self) -> StreamTemplateInfoList:
        """List stream templates.

        Lists the stream templates streams can be created from in the project, together with their limits
        and lifecycle settings. Templates with no streams provisioned are not included.

        Note:
            This endpoint is in alpha, and is called with the alpha ``cdf-version`` header.

        Returns:
            StreamTemplateInfoList: The available stream templates, sorted by name.

        Examples:

            List the stream templates available in the project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.streams.templates.list()
        """
        self._warning.warn()
        return await self._list(
            method="GET",
            list_cls=StreamTemplateInfoList,
            resource_cls=StreamTemplateInfo,
            headers=self._alpha_version_header(),
        )
