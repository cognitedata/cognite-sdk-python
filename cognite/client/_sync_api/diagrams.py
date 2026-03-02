"""
===============================================================================
2a6b1a73d904ab390a450220976961e1
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    DetectJobBundle,
    DiagramConvertResults,
    DiagramDetectConfig,
    DiagramDetectResults,
    FileReference,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncDiagramsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

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
        configuration: DiagramDetectConfig | None = None,
        *,
        multiple_jobs: Literal[False] = False,
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
        configuration: DiagramDetectConfig | None = None,
        *,
        multiple_jobs: Literal[True],
    ) -> tuple[DetectJobBundle, list[dict[str, Any]]]: ...

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
        configuration: DiagramDetectConfig | None = None,
        *,
        multiple_jobs: bool = False,
    ) -> DiagramDetectResults | tuple[DetectJobBundle, list[dict[str, Any]]]:
        """
        `Detect annotations in engineering diagrams <https://api-docs.cognite.com/20230101/tag/Engineering-diagrams/operation/diagramDetect>`_

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
            configuration (DiagramDetectConfig | None): Additional configuration for the detect algorithm. See `DiagramDetectConfig` class documentation and `beta API docs <https://api-docs.cognite.com/20230101-beta/tag/Engineering-diagrams/operation/diagramDetect/#!path=configuration&t=request>`_.
            multiple_jobs (bool): Enables you to publish multiple jobs. If True the method returns a tuple of DetectJobBundle and list of potentially unposted files. If False it will return a single DiagramDetectResults. Defaults to False.
        Returns:
            DiagramDetectResults | tuple[DetectJobBundle, list[dict[str, Any]]]: Resulting queued job or a bundle of jobs and a list of unposted files. Note that the .result property of the job or job bundle will block waiting for results.

        Note:
            The results are not written to CDF, to create annotations based on detected entities use `AnnotationsAPI`.

        Examples:
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.contextualization import FileReference
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
                >>> result = detect_job.get_result()
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
        return run_sync(
            self.__async_client.diagrams.detect(
                entities=entities,
                search_field=search_field,
                partial_match=partial_match,
                min_tokens=min_tokens,
                file_ids=file_ids,
                file_external_ids=file_external_ids,
                file_instance_ids=file_instance_ids,
                file_references=file_references,
                pattern_mode=pattern_mode,
                configuration=configuration,
                multiple_jobs=multiple_jobs,
            )  # type: ignore [call-overload, misc]
        )

    def get_detect_jobs(self, job_ids: list[int]) -> list[DiagramDetectResults]:
        return run_sync(self.__async_client.diagrams.get_detect_jobs(job_ids=job_ids))

    def convert(self, detect_job: DiagramDetectResults) -> DiagramConvertResults:
        """
        Convert a P&ID to interactive SVGs where the provided annotations are highlighted.

        Note:
            Will automatically wait for the detect job to complete before starting the conversion.

        Args:
            detect_job (DiagramDetectResults): detect job

        Returns:
            DiagramConvertResults: Resulting queued job.

        Examples:

            Run a detection job, then convert the results:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> detect_job = client.diagrams.detect(...)
                >>> client.diagrams.convert(detect_job=detect_job)
        """
        return run_sync(self.__async_client.diagrams.convert(detect_job=detect_job))
