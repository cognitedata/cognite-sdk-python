import warnings

from cognite.client._api_client import APIClient
from cognite.client.data_classes.contextualization import VisionExtractJob, VisionFeature
from cognite.client.utils._auxiliary import assert_type, to_camel_case
from cognite.client.utils._identifier import IdentifierSequence


class VisionAPI(APIClient):
    _RESOURCE_PATH = "/context/vision"

    @staticmethod
    def _process_file_ids(ids, external_ids):
        identifier_sequence = IdentifierSequence.load(ids=ids, external_ids=external_ids).as_primitives()
        id_objs = [{"fileId": id} for id in identifier_sequence if isinstance(id, int)]
        external_id_objs = [
            {"fileExternalId": external_id} for external_id in identifier_sequence if isinstance(external_id, str)
        ]
        return [*id_objs, *external_id_objs]

    def _run_job(self, job_path, job_cls, status_path=None, headers=None, **kwargs: Any):
        if status_path is None:
            status_path = job_path + "/"
        res = self._post(
            (self._RESOURCE_PATH + job_path),
            json={to_camel_case(k): v for (k, v) in (kwargs or {}).items() if (v is not None)},
            headers=headers,
        )
        return job_cls._load_with_status(
            res.json(), status_path=(self._RESOURCE_PATH + status_path), cognite_client=self._cognite_client
        )

    def extract(self, features, file_ids=None, file_external_ids=None, parameters=None):
        assert_type(features, "features", [VisionFeature, list], allow_none=False)
        if isinstance(features, list):
            for f in features:
                assert_type(f, f"feature '{f}'", [VisionFeature], allow_none=False)
        if isinstance(features, VisionFeature):
            features = [features]
        beta_features = [f for f in features if (f in VisionFeature.beta_features())]
        if len(beta_features) > 0:
            warnings.warn(f"Features {beta_features} are in beta and are still in development")
        return self._run_job(
            job_path="/extract",
            status_path="/extract/",
            items=self._process_file_ids(file_ids, file_external_ids),
            features=features,
            parameters=(parameters.dump(camel_case=True) if (parameters is not None) else None),
            job_cls=VisionExtractJob,
            headers=({"cdf-version": "beta"} if (len(beta_features) > 0) else None),
        )

    def get_extract_job(self, job_id):
        job = VisionExtractJob(
            job_id=job_id, status_path=f"{self._RESOURCE_PATH}/extract/", cognite_client=self._cognite_client
        )
        job.update_status()
        return job
