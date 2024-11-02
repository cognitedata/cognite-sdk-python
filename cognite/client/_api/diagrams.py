from __future__ import annotations

from collections.abc import Sequence
from math import ceil
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast, overload

from requests import Response

from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    DetectJobBundle,
    DiagramConvertResults,
    DiagramDetectConfig,
    DiagramDetectResults,
    FileReference,
    T_ContextualizationJob,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.exceptions import CogniteAPIError, CogniteMissingClientError
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._text import to_camel_case
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig

_T = TypeVar("_T")


class DiagramsAPI(APIClient):
    _RESOURCE_PATH = "/context/diagram"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        # https://developer.cognite.com/api#tag/Engineering-diagrams/operation/diagramDetect
        self._DETECT_API_FILE_LIMIT = 50
        self._DETECT_API_STATUS_JOB_LIMIT = 1000
        self._detect_beta_params_warning = FeaturePreviewWarning(
            api_maturity="beta",
            sdk_maturity="beta",
            feature_name="Support for diagram detect 'configuration' and 'pattern_mode' parameters",
        )

    def _camel_post(
        self,
        context_path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        api_subversion: str | None = None,
    ) -> Response:
        return self._post(
            self._RESOURCE_PATH + context_path,
            json={to_camel_case(k): v for k, v in (json or {}).items() if v is not None},
            params=params,
            headers=headers,
            api_subversion=api_subversion,
        )

    def _run_job(
        self,
        job_cls: type[T_ContextualizationJob],
        job_path: str,
        status_path: str | None = None,
        headers: dict[str, Any] | None = None,
        api_subversion: str | None = None,
        **kwargs: Any,
    ) -> T_ContextualizationJob:
        if status_path is None:
            status_path = job_path + "/"
        response = self._camel_post(job_path, json=kwargs, headers=headers, api_subversion=api_subversion)
        return job_cls._load_with_status(
            data=response.json(),
            headers=response.headers,
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )

    @staticmethod
    def _list_from_instance_or_list(
        instance_or_list: Sequence[_T] | _T | None, instance_type: type[_T], error_message: str
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
        ids: Sequence[int] | int | None,
        external_ids: SequenceNotStr[str] | str | None,
        instance_ids: Sequence[NodeId] | NodeId | None,
        file_references: Sequence[FileReference] | FileReference | None,
    ) -> list[
        dict[str, int | str | dict[str, str] | dict[str, int]]
        | dict[str, dict[str, str]]
        | dict[str, str]
        | dict[str, int]
    ]:
        ids = DiagramsAPI._list_from_instance_or_list(ids, int, "ids must be int or list of int")
        external_ids = cast(
            SequenceNotStr[str],
            DiagramsAPI._list_from_instance_or_list(external_ids, str, "external_ids must be str or list of str"),
        )
        instance_ids = DiagramsAPI._list_from_instance_or_list(
            instance_ids, NodeId, "instance_ids must be NodeId or list of NodeId"
        )
        file_references = DiagramsAPI._list_from_instance_or_list(
            file_references, FileReference, "file_references must be FileReference or list of FileReference"
        )
        # Handle empty lists
        if not (external_ids or ids or instance_ids or file_references):
            raise ValueError("No ids, external ids or file references specified")

        id_objs = [{"fileId": id} for id in ids]
        external_id_objs = [{"fileExternalId": external_id} for external_id in external_ids]
        instance_id_objs = [
            {"fileInstanceId": instance_id.dump(camel_case=True, include_instance_type=False)}
            for instance_id in instance_ids
        ]
        file_reference_objects = [file_reference.to_api_item() for file_reference in file_references]
        return [*id_objs, *external_id_objs, *instance_id_objs, *file_reference_objects]

    @overload
    def detect(
        self,
        entities: Sequence[dict | CogniteResource],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: int | Sequence[int] | None = None,
        file_external_ids: str | SequenceNotStr[str] | None = None,
        file_instance_ids: NodeId | Sequence[NodeId] | None = None,
        file_references: list[FileReference] | FileReference | None = None,
        pattern_mode: bool = False,
        configuration: dict[str, Any] | None = None,
        *,
        multiple_jobs: Literal[False],
    ) -> DiagramDetectResults: ...

    @overload
    def detect(
        self,
        entities: Sequence[dict | CogniteResource],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: int | Sequence[int] | None = None,
        file_external_ids: str | SequenceNotStr[str] | None = None,
        file_instance_ids: NodeId | Sequence[NodeId] | None = None,
        file_references: list[FileReference] | FileReference | None = None,
        pattern_mode: bool = False,
        configuration: DiagramDetectConfig | dict[str, Any] | None = None,
        *,
        multiple_jobs: Literal[True],
    ) -> tuple[DetectJobBundle | None, list[dict[str, Any]]]: ...

    @overload
    def detect(
        self,
        entities: Sequence[dict | CogniteResource],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: int | Sequence[int] | None = None,
        file_external_ids: str | SequenceNotStr[str] | None = None,
        file_instance_ids: NodeId | Sequence[NodeId] | None = None,
        file_references: list[FileReference] | FileReference | None = None,
        pattern_mode: bool = False,
        configuration: DiagramDetectConfig | dict[str, Any] | None = None,
    ) -> DiagramDetectResults: ...

    def detect(
        self,
        entities: Sequence[dict | CogniteResource],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: int | Sequence[int] | None = None,
        file_external_ids: str | SequenceNotStr[str] | None = None,
        file_instance_ids: NodeId | Sequence[NodeId] | None = None,
        file_references: list[FileReference] | FileReference | None = None,
        pattern_mode: bool | None = None,
        configuration: DiagramDetectConfig | dict[str, Any] | None = None,
        *,
        multiple_jobs: bool = False,
    ) -> DiagramDetectResults | tuple[DetectJobBundle | None, list[dict[str, Any]]]:
        """`Detect annotations in engineering diagrams <https://developer.cognite.com/api#tag/Engineering-diagrams/operation/diagramDetect>`_

        Note:
            All users on this CDF subscription with assets read-all and files read-all capabilities in the project,
            are able to access the data sent to this endpoint.

        Args:
            entities (Sequence[dict | CogniteResource]): List of entities to detect
            search_field (str): If entities is a list of dictionaries, this is the key to the values to detect in the PnId
            partial_match (bool): Allow for a partial match (e.g. missing prefix).
            min_tokens (int): Minimal number of tokens a match must be based on
            file_ids (int | Sequence[int] | None): ID of the files, should already be uploaded in the same tenant.
            file_external_ids (str | SequenceNotStr[str] | None): File external ids, alternative to file_ids and file_references.
            file_instance_ids (NodeId | Sequence[NodeId] | None): Files to detect in, specified by instance id.
            file_references (list[FileReference] | FileReference | None): File references (id, external_id or instance_id), and first_page and last_page to specify page ranges per file. Each reference can specify up to 50 pages. Providing a page range will also make the page count of the document a part of the response.
            pattern_mode (bool | None): If True, entities must be provided with a sample field. This enables detecting tags that are similar to the sample, but not necessarily identical. Defaults to None.
            configuration (DiagramDetectConfig | dict[str, Any] | None): Additional configuration for the detect algorithm. See `DiagramDetectConfig` class documentation and `beta API docs <https://api-docs.cognite.com/20230101-beta/tag/Engineering-diagrams/operation/diagramDetect/#!path=configuration&t=request>`_.
            multiple_jobs (bool): Enables you to publish multiple jobs. If True the method returns a tuple of DetectJobBundle and list of potentially unposted files. If False it will return a single DiagramDetectResults. Defaults to False.
        Returns:
            DiagramDetectResults | tuple[DetectJobBundle | None, list[dict[str, Any]]]: Resulting queued job or a bundle of jobs and a list of unposted files. Note that the .result property of the job or job bundle will block waiting for results.

        Note:
            The results are not written to CDF, to create annotations based on detected entities use `AnnotationsAPI`.

        Examples:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.contextualization import FileReference
                >>> client = CogniteClient()
                >>> detect_job = client.diagrams.detect(
                ...     entities=[
                ...         {"userDefinedField": "21PT1017","ignoredField": "AA11"},
                ...         {"userDefinedField": "21PT1018"}],
                ...     search_field="userDefinedField",
                ...     partial_match=True,
                ...     min_tokens=2,
                ...     file_ids=[101],
                ...     file_external_ids=["Test1"],
                ...     file_references=[
                ...         FileReference(id=20, first_page=1, last_page=10),
                ...         FileReference(external_id="ext_20", first_page=11, last_page=20)
                ...     ])
                >>> result = detect_job.result
                >>> print(result)
                <code>
                {
                    'items': [
                        {'fileId': 101, 'annotations': []},
                        {'fileExternalId': 'Test1', 'fileId: 1, 'annotations': []},
                        {'fileId': 20, 'fileExternalId': 'ext_20', 'annotations': [], 'pageCount': 17},
                        {
                            'fileId': 20,
                            'fileExternalId': 'ext_20',
                            'annotations': [
                                {
                                    'text': '21PT1017',
                                    'entities': [{"userDefinedField": "21PT1017","ignoredField": "AA11"}],
                                    'region': {
                                        'page': 12,
                                        'shape': 'rectangle',
                                        'vertices': [
                                            {'x': 0.01, 'y': 0.01},
                                            {'x': 0.01, 'y': 0.02},
                                            {'x': 0.02, 'y': 0.02},
                                            {'x': 0.02, 'y': 0.01}
                                        ]
                                    }
                                }
                            ],
                            'pageCount': 17
                        }
                    ]
                }
                </code>

            To use beta configuration options you can use a dictionary or `DiagramDetectConfig` object for convenience:

                >>> from cognite.client.data_classes.contextualization import ConnectionFlags, DiagramDetectConfig
                >>> config = DiagramDetectConfig(
                ...     remove_leading_zeros=True,
                ...     connection_flags=ConnectionFlags(
                ...         no_text_inbetween=True,
                ...         natural_reading_order=True,
                ...     )
                ... )
                >>> job = client.diagrams.detect(entities=[{"name": "A1"}], file_id=123, config=config)

            Check the documentation for `DiagramDetectConfig` for more information on the available options.
        """
        items = self._process_file_ids(file_ids, file_external_ids, file_instance_ids, file_references)
        entities = [
            entity.dump(camel_case=True) if isinstance(entity, CogniteResource) else entity for entity in entities
        ]
        api_subversion = None
        beta_parameters = {}
        if pattern_mode is not None or configuration is not None:
            config = configuration.dump() if isinstance(configuration, DiagramDetectConfig) else configuration
            beta_parameters = dict(pattern_mode=pattern_mode, configuration=config)

            self._detect_beta_params_warning.warn()
            if self._api_subversion and not self._api_subversion.endswith("beta"):
                api_subversion = f"{self._api_subversion}-beta"

        if multiple_jobs:
            num_new_jobs = ceil(len(items) / self._DETECT_API_FILE_LIMIT)
            if num_new_jobs > self._DETECT_API_STATUS_JOB_LIMIT:
                raise ValueError(
                    f"Number of jobs exceed limit of: '{self._DETECT_API_STATUS_JOB_LIMIT}'. Number of jobs: '{num_new_jobs}'"
                )

            jobs: list[DiagramDetectResults] = []
            unposted_files: list[dict[str, Any]] = []
            for i in range(num_new_jobs):
                batch = items[(self._DETECT_API_FILE_LIMIT * i) : self._DETECT_API_FILE_LIMIT * (i + 1)]

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
                        api_subversion=api_subversion,
                        **beta_parameters,  # type: ignore[arg-type]
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
            api_subversion=api_subversion,
            **beta_parameters,  # type: ignore[arg-type]
        )

    def get_detect_jobs(self, job_ids: list[int]) -> list[DiagramDetectResults]:
        if self._cognite_client is None:
            raise CogniteMissingClientError(self)
        res = self._cognite_client.diagrams._post("/context/diagram/detect/status", json={"items": job_ids})
        jobs = res.json()["items"]
        return [
            DiagramDetectResults._load_with_status(
                data=job,
                headers=res.headers,
                cognite_client=self._cognite_client,
                status_path="/context/diagram/detect/",
            )
            for job in jobs
        ]

    @staticmethod
    def _process_detect_job(detect_job: DiagramDetectResults) -> list:
        """process the result from detect job so it complies with diagram convert schema

        Args:
            detect_job (DiagramDetectResults): detect job

        Returns:
            list: the format complies with diagram convert schema
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
            detect_job (DiagramDetectResults): detect job

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
