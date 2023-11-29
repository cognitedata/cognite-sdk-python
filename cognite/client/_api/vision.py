from __future__ import annotations

import warnings
from typing import Any

from cognite.client._api_client import APIClient
from cognite.client.data_classes.contextualization import (
    FeatureParameters,
    T_ContextualizationJob,
    VisionExtractJob,
    VisionFeature,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._text import to_camel_case
from cognite.client.utils._validation import assert_type


class VisionAPI(APIClient):
    _RESOURCE_PATH = "/context/vision"

    @staticmethod
    def _process_file_ids(ids: list[int] | int | None, external_ids: list[str] | str | None) -> list:
        """
        Utility for sanitizing a given lists of ids and external ids.
        Returns the concatenation of the ids an external ids in the format
        expected by the Context API.

        Args:
            ids (list[int] | int | None): No description.
            external_ids (list[str] | str | None): No description.
        Returns:
            list: No description."""
        identifier_sequence = IdentifierSequence.load(ids=ids, external_ids=external_ids).as_primitives()
        id_objs = [{"fileId": id} for id in identifier_sequence if isinstance(id, int)]
        external_id_objs = [
            {"fileExternalId": external_id} for external_id in identifier_sequence if isinstance(external_id, str)
        ]
        return [*id_objs, *external_id_objs]

    def _run_job(
        self,
        job_path: str,
        job_cls: type[T_ContextualizationJob],
        status_path: str | None = None,
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> T_ContextualizationJob:
        if status_path is None:
            status_path = job_path + "/"
        res = self._post(
            self._RESOURCE_PATH + job_path,
            json={to_camel_case(k): v for k, v in (kwargs or {}).items() if v is not None},
            headers=headers,
        )
        return job_cls._load_with_status(
            data=res.json(),
            headers=res.headers,
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )

    def extract(
        self,
        features: VisionFeature | list[VisionFeature],
        file_ids: list[int] | None = None,
        file_external_ids: list[str] | None = None,
        parameters: FeatureParameters | None = None,
    ) -> VisionExtractJob:
        """`Start an asynchronous job to extract features from image files. <https://developer.cognite.com/api#tag/Vision/operation/postVisionExtract>`_

        Args:
            features (VisionFeature | list[VisionFeature]): The feature(s) to extract from the provided image files.
            file_ids (list[int] | None): IDs of the image files to analyze. The images must already be uploaded in the same CDF project.
            file_external_ids (list[str] | None): The external file ids of the image files to analyze.
            parameters (FeatureParameters | None): No description.
        Returns:
            VisionExtractJob: Resulting queued job, which can be used to retrieve the status of the job or the prediction results if the job is finished. Note that .result property of this job will wait for the job to finish and returns the results.

        Examples:
            Start a job, wait for completion and then get the parsed results::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.contextualization import VisionFeature
                >>> c = CogniteClient()
                >>> extract_job = c.vision.extract(features=VisionFeature.ASSET_TAG_DETECTION, file_ids=[1])
                >>> extract_job.wait_for_completion()
                >>> for item in extract_job.items:
                ...     predictions = item.predictions
                ...     # do something with the predictions
                >>> # Save predictions in CDF using Annotations API:
                >>> extract_job.save_predictions()
        """
        # Sanitize input(s)
        assert_type(features, "features", [VisionFeature, list], allow_none=False)
        if isinstance(features, list):
            for f in features:
                assert_type(f, f"feature '{f}'", [VisionFeature], allow_none=False)
        if isinstance(features, VisionFeature):
            features = [features]

        beta_features = [f for f in features if f in VisionFeature.beta_features()]
        if len(beta_features) > 0:
            warnings.warn(f"Features {beta_features} are in beta and are still in development")

        return self._run_job(
            job_path="/extract",
            status_path="/extract/",
            items=self._process_file_ids(file_ids, file_external_ids),
            features=features,
            parameters=parameters
            if isinstance(parameters, dict)
            else parameters.dump(camel_case=True)
            if parameters is not None
            else None,
            job_cls=VisionExtractJob,
            headers={"cdf-version": "beta"} if len(beta_features) > 0 else None,
        )

    def get_extract_job(self, job_id: int) -> VisionExtractJob:
        """`Retrieve an existing extract job by ID. <https://developer.cognite.com/api#tag/Vision/operation/getVisionExtract>`_

        Args:
            job_id (int): ID of an existing feature extraction job.

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
