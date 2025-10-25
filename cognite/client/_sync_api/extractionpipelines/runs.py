"""
===============================================================================
c7e0250a7afdf41370a375942043efcf
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, overload

from cognite.client import AsyncCogniteClient
from cognite.client._api.extractionpipelines import RunStatus
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    ExtractionPipelineRun,
    ExtractionPipelineRunList,
    TimestampRange,
)
from cognite.client.data_classes.extractionpipelines import (
    ExtractionPipelineRunWrite,
)
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncExtractionPipelineRunsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def list(
        self,
        external_id: str,
        statuses: RunStatus | Sequence[RunStatus] | SequenceNotStr[str] | None = None,
        message_substring: str | None = None,
        created_time: dict[str, Any] | TimestampRange | str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ExtractionPipelineRunList:
        """
        `List runs for an extraction pipeline with given external_id <https://developer.cognite.com/api#tag/Extraction-Pipelines-Runs/operation/filterRuns>`_

        Args:
            external_id (str): Extraction pipeline external Id.
            statuses (RunStatus | Sequence[RunStatus] | SequenceNotStr[str] | None): One or more among "success" / "failure" / "seen".
            message_substring (str | None): Failure message part.
            created_time (dict[str, Any] | TimestampRange | str | None): Range between two timestamps. Possible keys are `min` and `max`, with values given as timestamps in ms.
                If a string is passed, it is assumed to be the minimum value.
            limit (int | None): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ExtractionPipelineRunList: List of requested extraction pipeline runs

        Tip:
            The ``created_time`` parameter can also be passed as a string, to support the most typical usage pattern
            of fetching the most recent runs, meaning it is implicitly assumed to be the minimum created time. The
            format is "N[timeunit]-ago", where timeunit is w,d,h,m (week, day, hour, minute), e.g. "12d-ago".

        Examples:

            List extraction pipeline runs:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> runsList = client.extraction_pipelines.runs.list(external_id="test ext id", limit=5)

            Filter extraction pipeline runs on a given status:

                >>> runs_list = client.extraction_pipelines.runs.list(external_id="test ext id", statuses=["seen"], limit=5)

            Get all failed pipeline runs in the last 24 hours for pipeline 'extId':

                >>> from cognite.client.data_classes import ExtractionPipelineRun
                >>> res = client.extraction_pipelines.runs.list(external_id="extId", statuses="failure", created_time="24h-ago")
        """
        return run_sync(
            self.__async_client.extraction_pipelines.runs.list(
                external_id=external_id,
                statuses=statuses,
                message_substring=message_substring,
                created_time=created_time,
                limit=limit,
            )
        )

    @overload
    def create(self, run: ExtractionPipelineRun | ExtractionPipelineRunWrite) -> ExtractionPipelineRun: ...

    @overload
    def create(
        self, run: Sequence[ExtractionPipelineRun] | Sequence[ExtractionPipelineRunWrite]
    ) -> ExtractionPipelineRunList: ...

    def create(
        self,
        run: ExtractionPipelineRun
        | ExtractionPipelineRunWrite
        | Sequence[ExtractionPipelineRun]
        | Sequence[ExtractionPipelineRunWrite],
    ) -> ExtractionPipelineRun | ExtractionPipelineRunList:
        """
        `Create one or more extraction pipeline runs. <https://developer.cognite.com/api#tag/Extraction-Pipelines-Runs/operation/createRuns>`_

        You can create an arbitrary number of extraction pipeline runs, and the SDK will split the request into multiple requests.

        Args:
            run (ExtractionPipelineRun | ExtractionPipelineRunWrite | Sequence[ExtractionPipelineRun] | Sequence[ExtractionPipelineRunWrite]): ExtractionPipelineRun| ExtractionPipelineRunWrite | Sequence[ExtractionPipelineRun]  | Sequence[ExtractionPipelineRunWrite]): Extraction pipeline or list of extraction pipeline runs to create.

        Returns:
            ExtractionPipelineRun | ExtractionPipelineRunList: Created extraction pipeline run(s)

        Examples:

            Report a new extraction pipeline run:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineRunWrite
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.runs.create(
                ...     ExtractionPipelineRunWrite(status="success", extpipe_external_id="extId"))
        """
        return run_sync(self.__async_client.extraction_pipelines.runs.create(run=run))
