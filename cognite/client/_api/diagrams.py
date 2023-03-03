
from math import ceil
from requests import Response
from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import DetectJobBundle, DiagramConvertResults, DiagramDetectResults, FileReference, T_ContextualizationJob
from cognite.client.exceptions import CogniteAPIError, CogniteMissingClientError
from cognite.client.utils._auxiliary import to_camel_case
DETECT_API_FILE_LIMIT = 50
DETECT_API_STATUS_JOB_LIMIT = 1000
_T = TypeVar('_T')

class DiagramsAPI(APIClient):
    _RESOURCE_PATH = '/context/diagram'

    def _camel_post(self, context_path, json=None, params=None, headers=None):
        return self._post((self._RESOURCE_PATH + context_path), json={to_camel_case(k): v for (k, v) in (json or {}).items() if (v is not None)}, params=params, headers=headers)

    def _run_job(self, job_cls, job_path, status_path=None, headers=None, **kwargs: Any):
        if (status_path is None):
            status_path = (job_path + '/')
        return job_cls._load_with_status(self._camel_post(job_path, json=kwargs, headers=headers).json(), status_path=(self._RESOURCE_PATH + status_path), cognite_client=self._cognite_client)

    @staticmethod
    def _list_from_instance_or_list(instance_or_list, instance_type, error_message):
        if (instance_or_list is None):
            return []
        if isinstance(instance_or_list, instance_type):
            return [instance_or_list]
        if (isinstance(instance_or_list, list) and all((isinstance(x, instance_type) for x in instance_or_list))):
            return instance_or_list
        raise TypeError(error_message)

    @staticmethod
    def _process_file_ids(ids, external_ids, file_references):
        ids = DiagramsAPI._list_from_instance_or_list(ids, int, 'ids must be int or list of int')
        external_ids = DiagramsAPI._list_from_instance_or_list(external_ids, str, 'external_ids must be str or list of str')
        file_references = DiagramsAPI._list_from_instance_or_list(file_references, FileReference, 'file_references must be FileReference or list of FileReference')
        if (not (external_ids or ids or file_references)):
            raise ValueError('No ids, external ids or file references specified')
        id_objs = [{'fileId': id} for id in ids]
        external_id_objs = [{'fileExternalId': external_id} for external_id in external_ids]
        file_reference_objects = [file_reference.to_api_item() for file_reference in file_references]
        return [*id_objs, *external_id_objs, *file_reference_objects]

    @overload
    def detect(self, entities, search_field='name', partial_match=False, min_tokens=2, file_ids=None, file_external_ids=None, file_references=None, *, multiple_jobs: Literal[False]):
        ...

    @overload
    def detect(self, entities, search_field='name', partial_match=False, min_tokens=2, file_ids=None, file_external_ids=None, file_references=None, *, multiple_jobs: Literal[True]):
        ...

    @overload
    def detect(self, entities, search_field='name', partial_match=False, min_tokens=2, file_ids=None, file_external_ids=None, file_references=None):
        ...

    def detect(self, entities, search_field='name', partial_match=False, min_tokens=2, file_ids=None, file_external_ids=None, file_references=None, *, multiple_jobs: bool=False):
        items = self._process_file_ids(file_ids, file_external_ids, file_references)
        entities = [(entity.dump(camel_case=True) if isinstance(entity, CogniteResource) else entity) for entity in entities]
        if multiple_jobs:
            num_new_jobs = ceil((len(items) / DETECT_API_FILE_LIMIT))
            if (num_new_jobs > DETECT_API_STATUS_JOB_LIMIT):
                raise ValueError(f"Number of jobs exceed limit of: '{DETECT_API_STATUS_JOB_LIMIT}'. Number of jobs: '{num_new_jobs}'")
            jobs: List[DiagramDetectResults] = []
            unposted_files: List[Dict[(str, Any)]] = []
            for i in range(num_new_jobs):
                batch = items[(DETECT_API_FILE_LIMIT * i):(DETECT_API_FILE_LIMIT * (i + 1))]
                try:
                    posted_job = self._run_job(job_path='/detect', status_path='/detect/', items=batch, entities=entities, partial_match=partial_match, search_field=search_field, min_tokens=min_tokens, job_cls=DiagramDetectResults)
                    jobs.append(posted_job)
                except CogniteAPIError as exc:
                    unposted_files.append({'error': str(exc), 'files': batch})
                res = (DetectJobBundle(cognite_client=self._cognite_client, job_ids=[j.job_id for j in jobs if j.job_id]) if jobs else None)
            return (res, unposted_files)
        return self._run_job(job_path='/detect', status_path='/detect/', items=items, entities=entities, partial_match=partial_match, search_field=search_field, min_tokens=min_tokens, job_cls=DiagramDetectResults)

    def get_detect_jobs(self, job_ids):
        if (self._cognite_client is None):
            raise CogniteMissingClientError
        res = self._cognite_client.diagrams._post('/context/diagram/detect/status', json={'items': job_ids})
        jobs = res.json()['items']
        return [DiagramDetectResults._load_with_status(data=job, cognite_client=self._cognite_client, status_path='/context/diagram/detect/') for job in jobs]

    @staticmethod
    def _process_detect_job(detect_job):
        if any(((item.get('page_range') is not None) for item in detect_job.result['items'])):
            raise NotImplementedError('Can not run convert on a detect job that used the page range feature')
        items = [{k: v for (k, v) in item.items() if (k in {'annotations', 'fileId'})} for item in detect_job.result['items']]
        return items

    def convert(self, detect_job):
        return self._run_job(job_path='/convert', status_path='/convert/', items=self._process_detect_job(detect_job), job_cls=DiagramConvertResults)
