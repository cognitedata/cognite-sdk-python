from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    ExtractionPipelineConfig,
    ExtractionPipelineConfigRevisionList,
)
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineConfigWrite
from cognite.client.utils._auxiliary import drop_none_values


class ExtractionPipelineConfigsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/config"

    async def retrieve(
        self, external_id: str, revision: int | None = None, active_at_time: int | None = None
    ) -> ExtractionPipelineConfig:
        """`Retrieve a specific configuration revision, or the latest by default <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/getExtPipeConfigRevision>`

        By default the latest configuration revision is retrieved, or you can specify a timestamp or a revision number.

        Args:
            external_id (str): External id of the extraction pipeline to retrieve config from.
            revision (int | None): Optionally specify a revision number to retrieve.
            active_at_time (int | None): Optionally specify a timestamp the configuration revision should be active.

        Returns:
            ExtractionPipelineConfig: Retrieved extraction pipeline configuration revision

        Examples:

            Retrieve latest config revision:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.extraction_pipelines.config.retrieve("extId")
        """
        response = await self._get(
            self._RESOURCE_PATH,
            params=drop_none_values({"externalId": external_id, "activeAtTime": active_at_time, "revision": revision}),
        )
        return ExtractionPipelineConfig._load(response.json())

    async def list(self, external_id: str) -> ExtractionPipelineConfigRevisionList:
        """`Retrieve all configuration revisions from an extraction pipeline <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/listExtPipeConfigRevisions>`

        Args:
            external_id (str): External id of the extraction pipeline to retrieve config from.

        Returns:
            ExtractionPipelineConfigRevisionList: Retrieved extraction pipeline configuration revisions

        Examples:

            Retrieve a list of config revisions:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.extraction_pipelines.config.list("extId")
        """
        response = await self._get(f"{self._RESOURCE_PATH}/revisions", params={"externalId": external_id})
        return ExtractionPipelineConfigRevisionList._load(response.json()["items"])

    async def create(
        self, config: ExtractionPipelineConfig | ExtractionPipelineConfigWrite
    ) -> ExtractionPipelineConfig:
        """`Create a new configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/createExtPipeConfig>`

        Args:
            config (ExtractionPipelineConfig | ExtractionPipelineConfigWrite): Configuration revision to create.

        Returns:
            ExtractionPipelineConfig: Created extraction pipeline configuration revision

        Examples:

            Create a config revision:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineConfigWrite
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.config.create(ExtractionPipelineConfigWrite(external_id="extId", config="my config contents"))
        """
        if isinstance(config, ExtractionPipelineConfig):
            config = config.as_write()
        response = await self._post(self._RESOURCE_PATH, json=config.dump(camel_case=True))
        return ExtractionPipelineConfig._load(response.json())

    async def revert(self, external_id: str, revision: int) -> ExtractionPipelineConfig:
        """`Revert to a previous configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/revertExtPipeConfigRevision>`

        Args:
            external_id (str): External id of the extraction pipeline to revert revision for.
            revision (int): Revision to revert to.

        Returns:
            ExtractionPipelineConfig: New latest extraction pipeline configuration revision.

        Examples:

            Revert a config revision:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.extraction_pipelines.config.revert("extId", 5)
        """
        response = await self._post(
            f"{self._RESOURCE_PATH}/revert", json={"externalId": external_id, "revision": revision}
        )
        return ExtractionPipelineConfig._load(response.json())
