"""
===============================================================================
3f683d6b90598c40be474b7f0a5f045d
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import ExtractionPipelineConfig, ExtractionPipelineConfigRevisionList
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineConfigWrite
from cognite.client.utils._async_helpers import run_sync


class SyncExtractionPipelineConfigsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve(
        self, external_id: str, revision: int | None = None, active_at_time: int | None = None
    ) -> ExtractionPipelineConfig:
        """
        `Retrieve a specific configuration revision, or the latest by default <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines-Config/operation/getExtPipeConfigRevision>`

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
        return run_sync(
            self.__async_client.extraction_pipelines.config.retrieve(
                external_id=external_id, revision=revision, active_at_time=active_at_time
            )
        )

    def list(self, external_id: str) -> ExtractionPipelineConfigRevisionList:
        """
        `Retrieve all configuration revisions from an extraction pipeline <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines-Config/operation/listExtPipeConfigRevisions>`

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
        return run_sync(self.__async_client.extraction_pipelines.config.list(external_id=external_id))

    def create(self, config: ExtractionPipelineConfig | ExtractionPipelineConfigWrite) -> ExtractionPipelineConfig:
        """
        `Create a new configuration revision <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines-Config/operation/createExtPipeConfig>`

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
        return run_sync(self.__async_client.extraction_pipelines.config.create(config=config))

    def revert(self, external_id: str, revision: int) -> ExtractionPipelineConfig:
        """
        `Revert to a previous configuration revision <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines-Config/operation/revertExtPipeConfigRevision>`

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
        return run_sync(
            self.__async_client.extraction_pipelines.config.revert(external_id=external_id, revision=revision)
        )
