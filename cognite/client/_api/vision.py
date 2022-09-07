from typing import Any, List, Optional, Union

from cognite.client._api._context_client import ContextAPI
from cognite.client.data_classes.contextualization import ContextualizationJob
from cognite.client.data_classes.vision import Feature, FeatureParameters, InternalId, VisionExtractJob
from cognite.client.utils._auxiliary import assert_type


class VisionAPI(ContextAPI):
    _RESOURCE_PATH = "/context/vision"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def extract(
        self,
        features: Union[Feature, List[Feature]],
        file_ids: Optional[List[int]] = None,
        file_external_ids: Optional[List[str]] = None,
        parameters: Optional[FeatureParameters] = None,
    ) -> ContextualizationJob:
        """Start an asynchronous job to extract features from image files.

        Args:
            features (Union[Feature, List[Feature]]): The feature(s) to extract from the provided image files.
            file_ids (List[int]): IDs of the image files to analyze. The images must already be uploaded in the same CDF project.
            file_external_ids (List[str]): The external file ids of the image files to analyze.
        Returns:
            VisionExtractJob: Resulting queued job, which can be used to retrieve the status of the job or the prediction results if the job is finished. Note that .result property of this job will wait for the job to finish and returns the results.

        Examples:
            Start a job, wait for completion and then get the parsed results::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.vision import Feature
                >>> c = CogniteClient()
                >>> extract_job = c.vision.extract(features=Feature.ASSET_TAG_DETECTION, file_ids=[1])
                >>> extract_job.wait_for_completion()
                >>> for item in extract_job.items:
                ...     predictions = item.predictions
                ...     # do something with the predictions
                >>> # Save predictions in CDF using Annotations API:
                >>> extract_job.save_predictions()
        """
        # Sanitize input(s)
        assert_type(features, "features", [Feature, list], allow_none=False)
        if isinstance(features, list):
            for f in features:
                assert_type(f, f"feature '{f}'", [Feature], allow_none=False)
        if isinstance(features, Feature):
            features = [features]

        return self._run_job(
            job_path="/extract",
            status_path="/extract/",
            items=self._process_file_ids(file_ids, file_external_ids),
            features=features,
            parameters=parameters.dump(camel_case=True) if parameters is not None else None,
            job_cls=VisionExtractJob,
        )

    def get_extract_job(self, job_id: InternalId) -> ContextualizationJob:
        """Retrieve an existing extract job by ID.

        Args:
            job_id (InternalId): ID of an existing feature extraction job.
        Returns:
            VisionExtractJob: Vision extract job, which can be used to retrieve the status of the job or the prediction results if the job is finished. Note that .result property of this job will wait for the job to finish and returns the results.

        Examples:
            Retrieve a vision extract job by ID::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> extract_job = c.vision.get_extract_job(job_id=1)
                >>> extract_job.wait_for_completion()
                >>> for item in extract_job.items:
                ...     predictions = item.predictions
                ...     # do something with the predictions
        """
        job = VisionExtractJob(
            job_id=job_id,
            status_path=f"{self._RESOURCE_PATH}/extract/",
            cognite_client=self._cognite_client,
        )
        job.update_status()

        return job
