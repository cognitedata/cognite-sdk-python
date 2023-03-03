from math import ceil
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Type, TypeVar, Union, overload

from requests import Response

from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    DetectJobBundle,
    DiagramConvertResults,
    DiagramDetectResults,
    FileReference,
    T_ContextualizationJob,
)
from cognite.client.exceptions import CogniteAPIError, CogniteMissingClientError
from cognite.client.utils._auxiliary import to_camel_case

DETECT_API_FILE_LIMIT = 50
# https://docs.cognite.com/api/playground/#tag/Engineering-diagrams/operation/diagramDetect
DETECT_API_STATUS_JOB_LIMIT = 1000
# https://docs.cognite.com/api/playground/#tag/Engineering-diagrams/operation/diagramDetectMultipleResults

_T = TypeVar("_T")


class DiagramsAPI(APIClient):
    _RESOURCE_PATH = "/context/diagram"

    def _camel_post(
        self,
        context_path: str,
        json: Dict[str, Any] = None,
        params: Dict[str, Any] = None,
        headers: Dict[str, Any] = None,
    ) -> Response:
        return self._post(
            self._RESOURCE_PATH + context_path,
            json={to_camel_case(k): v for k, v in (json or {}).items() if v is not None},
            params=params,
            headers=headers,
        )

    def _run_job(
        self,
        job_cls: Type[T_ContextualizationJob],
        job_path: str,
        status_path: str = None,
        headers: Dict[str, Any] = None,
        **kwargs: Any,
    ) -> T_ContextualizationJob:
        if status_path is None:
            status_path = job_path + "/"
        return job_cls._load_with_status(
            self._camel_post(job_path, json=kwargs, headers=headers).json(),
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )

    @staticmethod
    def _list_from_instance_or_list(
        instance_or_list: Union[Sequence[_T], _T, None], instance_type: Type[_T], error_message: str
    ) -> Sequence[_T]:
        if instance_or_list is None:
            return []
        if isinstance(instance_or_list, instance_type):
            return [instance_or_list]
        if isinstance(instance_or_list, list) and all(isinstance(x, instance_type) for x in instance_or_list):
            return instance_or_list
        raise TypeError(error_message)

    @staticmethod
    def _process_file_ids(
        ids: Union[Sequence[int], int, None],
        external_ids: Union[Sequence[str], str, None],
        file_references: Union[Sequence[FileReference], FileReference, None],
    ) -> List[Union[Dict[str, Union[int, str, Dict[str, int]]], Dict[str, str], Dict[str, int]]]:

        ids = DiagramsAPI._list_from_instance_or_list(ids, int, "ids must be int or list of int")
        external_ids = DiagramsAPI._list_from_instance_or_list(
            external_ids, str, "external_ids must be str or list of str"
        )
        file_references = DiagramsAPI._list_from_instance_or_list(
            file_references, FileReference, "file_references must be FileReference or list of FileReference"
        )
        # Handle empty lists
        if not (external_ids or ids or file_references):
            raise ValueError("No ids, external ids or file references specified")

        id_objs = [{"fileId": id} for id in ids]
        external_id_objs = [{"fileExternalId": external_id} for external_id in external_ids]
        file_reference_objects = [file_reference.to_api_item() for file_reference in file_references]
        return [*id_objs, *external_id_objs, *file_reference_objects]

    @overload
    def detect(
        self,
        entities: Sequence[Union[dict, CogniteResource]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: Optional[Union[int, Sequence[int]]] = None,
        file_external_ids: Optional[Union[str, Sequence[str]]] = None,
        file_references: Union[List[FileReference], FileReference, None] = None,
        *,
        multiple_jobs: Literal[False],
    ) -> DiagramDetectResults:
        ...

    @overload
    def detect(
        self,
        entities: Sequence[Union[dict, CogniteResource]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: Optional[Union[int, Sequence[int]]] = None,
        file_external_ids: Optional[Union[str, Sequence[str]]] = None,
        file_references: Union[List[FileReference], FileReference, None] = None,
        *,
        multiple_jobs: Literal[True],
    ) -> Tuple[Optional[DetectJobBundle], List[Dict[str, Any]]]:
        ...

    @overload
    def detect(
        self,
        entities: Sequence[Union[dict, CogniteResource]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: Optional[Union[int, Sequence[int]]] = None,
        file_external_ids: Optional[Union[str, Sequence[str]]] = None,
        file_references: Union[List[FileReference], FileReference, None] = None,
    ) -> DiagramDetectResults:
        ...

    def detect(
        self,
        entities: Sequence[Union[dict, CogniteResource]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: Optional[Union[int, Sequence[int]]] = None,
        file_external_ids: Optional[Union[str, Sequence[str]]] = None,
        file_references: Union[List[FileReference], FileReference, None] = None,
        *,
        multiple_jobs: bool = False,
    ) -> Union[DiagramDetectResults, Tuple[Optional[DetectJobBundle], List[Dict[str, Any]]]]:

        """Detect entities in a PNID. The results are not written to CDF.

        Note:
            All users on this CDF subscription with assets read-all and files read-all capabilities in the project,
            are able to access the data sent to this endpoint.

        Args:
            entities (Sequence[Union[dict, CogniteResource]]): List of entities to detect
            search_field (str): If entities is a list of dictionaries, this is the key to the values to detect in the PnId
            partial_match (bool): Allow for a partial match (e.g. missing prefix).
            min_tokens (int): Minimal number of tokens a match must be based on
            file_ids (Sequence[int]): ID of the files, should already be uploaded in the same tenant.
            file_external_ids (Sequence[str]): File external ids.
            file_references (Sequence[FileReference]): File references (id or external id) with page ranges.
        Keyword Args:
            multiple_jobs (bool): Enables you to publish multiple jobs. If True the method will return a tuple of DetectJobBundle and list
                of potentially unposted files. If False it will return a single DiagramDetectResults. Defaults to False.
        Returns:
            Union[DiagramDetectResults, Tuple[DetectJobBundle, List[Dict[str, Any]]]: Resulting queued job or a bundle of jobs and a list of unposted files.
            Note that the .result property of the job or job bundle will block waiting for results.
        Examples:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> retrieved_model = client.diagrams.detect(
                    entities=[{"userDefinedField": "21PT1017","ignoredField": "AA11"}],
                    search_field="userDefinedField",
                    partial_match=True,
                    min_tokens=2,
                    file_ids=[101],
                    file_external_ids=["Test1"],
                )
        """
        items = self._process_file_ids(file_ids, file_external_ids, file_references)
        entities = [
            entity.dump(camel_case=True) if isinstance(entity, CogniteResource) else entity for entity in entities
        ]
        if multiple_jobs:
            num_new_jobs = ceil(len(items) / DETECT_API_FILE_LIMIT)
            if num_new_jobs > DETECT_API_STATUS_JOB_LIMIT:
                raise ValueError(
                    f"Number of jobs exceed limit of: '{DETECT_API_STATUS_JOB_LIMIT}'. Number of jobs: '{num_new_jobs}'"
                )

            jobs: List[DiagramDetectResults] = []
            unposted_files: List[Dict[str, Any]] = []
            for i in range(num_new_jobs):
                batch = items[(DETECT_API_FILE_LIMIT * i) : DETECT_API_FILE_LIMIT * (i + 1)]

                try:
                    posted_job = self._run_job(
                        job_path="/detect",
                        status_path="/detect/",
                        items=batch,
                        entities=entities,
                        partial_match=partial_match,
                        search_field=search_field,
                        min_tokens=min_tokens,
                        job_cls=DiagramDetectResults,
                    )
                    jobs.append(posted_job)
                except CogniteAPIError as exc:
                    unposted_files.append({"error": str(exc), "files": batch})

                res = (
                    DetectJobBundle(cognite_client=self._cognite_client, job_ids=[j.job_id for j in jobs if j.job_id])
                    if jobs
                    else None
                )
            return res, unposted_files

        return self._run_job(
            job_path="/detect",
            status_path="/detect/",
            items=items,
            entities=entities,
            partial_match=partial_match,
            search_field=search_field,
            min_tokens=min_tokens,
            job_cls=DiagramDetectResults,
        )

    def get_detect_jobs(self, job_ids: List[int]) -> List[DiagramDetectResults]:
        if self._cognite_client is None:
            raise CogniteMissingClientError
        res = self._cognite_client.diagrams._post("/context/diagram/detect/status", json={"items": job_ids})
        jobs = res.json()["items"]
        return [
            DiagramDetectResults._load_with_status(
                data=job, cognite_client=self._cognite_client, status_path="/context/diagram/detect/"
            )
            for job in jobs
        ]

    @staticmethod
    def _process_detect_job(detect_job: DiagramDetectResults) -> list:
        """process the result from detect job so it complies with diagram convert schema

        Args:
            detect_job (DiagramDetectResults): detect job

        Returns:
            items: the format complies with diagram convert schema
        """
        if any(item.get("page_range") is not None for item in detect_job.result["items"]):
            raise NotImplementedError("Can not run convert on a detect job that used the page range feature")
        items = [
            {k: v for k, v in item.items() if k in {"annotations", "fileId"}} for item in detect_job.result["items"]
        ]  # diagram detect always return file id.
        return items

    def convert(self, detect_job: DiagramDetectResults) -> DiagramConvertResults:
        """Convert a P&ID to interactive SVGs where the provided annotations are highlighted.

        Args:
            detect_job(DiagramConvertResults): detect job

        Returns:
            DiagramConvertResults: Resulting queued job. Note that .result property of this job will block waiting for results.
        Examples:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> detect_job = client.diagrams.detect(...)
                >>> client.diagrams.convert(detect_job=detect_job)

        """
        return self._run_job(
            job_path="/convert",
            status_path="/convert/",
            items=self._process_detect_job(detect_job),
            job_cls=DiagramConvertResults,
        )
