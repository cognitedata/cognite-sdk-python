"""
===============================================================================
d33f283ca186444bae989486c0978069
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.contextualization import FeatureParameters, VisionExtractJob, VisionFeature
from cognite.client.utils._async_helpers import run_sync


class SyncVisionAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def extract(
        self,
        features: VisionFeature | list[VisionFeature],
        file_ids: list[int] | None = None,
        file_external_ids: list[str] | None = None,
        parameters: FeatureParameters | None = None,
    ) -> VisionExtractJob:
        """
        `Start an asynchronous job to extract features from image files. <https://api-docs.cognite.com/20230101/tag/Vision/operation/postVisionExtract>`_

        Args:
            features (VisionFeature | list[VisionFeature]): The feature(s) to extract from the provided image files.
            file_ids (list[int] | None): IDs of the image files to analyze. The images must already be uploaded in the same CDF project.
            file_external_ids (list[str] | None): The external file ids of the image files to analyze.
            parameters (FeatureParameters | None): No description.
        Returns:
            VisionExtractJob: Resulting queued job, which can be used to retrieve the status of the job or the prediction results if the job is finished. Note that .result property of this job will wait for the job to finish and returns the results.

        Examples:
            Start a job, wait for completion and then get the parsed results:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.contextualization import VisionFeature
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> extract_job = client.vision.extract(features=VisionFeature.ASSET_TAG_DETECTION, file_ids=[1])
                >>> extract_job.wait_for_completion()
                >>> for item in extract_job.items:
                ...     predictions = item.predictions
                ...     # do something with the predictions
                >>> # Save predictions in CDF using Annotations API:
                >>> extract_job.save_predictions()
        """
        return run_sync(
            self.__async_client.vision.extract(
                features=features, file_ids=file_ids, file_external_ids=file_external_ids, parameters=parameters
            )
        )

    def get_extract_job(self, job_id: int) -> VisionExtractJob:
        """
        `Retrieve an existing extract job by ID. <https://api-docs.cognite.com/20230101/tag/Vision/operation/getVisionExtract>`_

        Args:
            job_id (int): ID of an existing feature extraction job.

        Returns:
            VisionExtractJob: Vision extract job, which can be used to retrieve the status of the job or the prediction results if the job is finished. Note that .result property of this job will wait for the job to finish and returns the results.

        Examples:
            Retrieve a vision extract job by ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> extract_job = client.vision.get_extract_job(job_id=1)
                >>> extract_job.wait_for_completion()
                >>> for item in extract_job.items:
                ...     predictions = item.predictions
                ...     # do something with the predictions
        """
        return run_sync(self.__async_client.vision.get_extract_job(job_id=job_id))
