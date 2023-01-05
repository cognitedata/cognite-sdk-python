from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import TYPE_CHECKING, Dict, Optional, Union, cast

from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteResourceList
from cognite.client.data_classes.transformations.common import TransformationDestination, _load_destination_dct

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationJobStatus(str, Enum):
    RUNNING = "Running"
    CREATED = "Created"
    COMPLETED = "Completed"
    FAILED = "Failed"


class TransformationJobMetric(CogniteResource):
    """The transformation job metric resource allows following details of execution of a transformation run.

    Args:
        timestamp (int): Time of the last metric update.
        name (str): Name of the metric.
        count (int): Value of the metric.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        timestamp: int = None,
        name: str = None,
        count: int = None,
        cognite_client: CogniteClient = None,
    ):
        self.timestamp = timestamp
        self.name = name
        self.count = count
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client: CogniteClient = None) -> TransformationJobMetric:
        instance = super()._load(resource, cognite_client)
        return instance


class TransformationJobMetricList(CogniteResourceList):
    _RESOURCE = TransformationJobMetric


class TransformationJob(CogniteResource):
    """The transformation job resource allows following the status of execution of a transformation run.

    Args:
        id (int): A server-generated ID for the object.
        status (TransformationJobStatus): Status of the job.
        transformation_id (int): Server-generated ID of the transformation.
        transformation_external_id (str): external ID of the transformation.
        source_project (str): Name of the CDF project the data will be read from.
        destination_project (str): Name of the CDF project the data will be written to.
        destination_type (str): Target resource type of the transformation.
        destination_database (str): Target database if the destination type is raw.
        destination_table (str): Target table name if the destination type is RAW.
        conflict_mode (str): What to do in case of id collisions: either "abort", "upsert", "update" or "delete".
        query (str): Query of the transformation that is being executed.
        error (str): Error message from the server.
        ignore_null_fields (bool): Indicates how null values are handled on updates: ignore or set null.
        created_time (int): Time when the job was created.
        started_time (int): Time when the job started running.
        finished_time (int): Time when the job finished running.
        last_seen_time (int): Time of the last status update from the job.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        status: TransformationJobStatus = None,
        transformation_id: int = None,
        transformation_external_id: str = None,
        source_project: str = None,
        destination_project: str = None,
        destination: TransformationDestination = None,
        conflict_mode: str = None,
        query: str = None,
        error: str = None,
        ignore_null_fields: bool = False,
        created_time: int = None,
        started_time: int = None,
        finished_time: int = None,
        last_seen_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        self.id = id
        self.status = status
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
        self.source_project = source_project
        self.destination_project = destination_project
        self.destination = destination
        self.conflict_mode = conflict_mode
        self.query = query
        self.error = error
        self.ignore_null_fields = ignore_null_fields
        self.created_time = created_time
        self.started_time = started_time
        self.finished_time = finished_time
        self.last_seen_time = last_seen_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def update(self) -> None:
        """`Get updated job status.`"""
        updated = self._cognite_client.transformations.jobs.retrieve(id=self.id)
        self.status = updated.status
        self.error = updated.error
        self.started_time = updated.started_time
        self.finished_time = updated.finished_time
        self.last_seen_time = updated.last_seen_time

    def cancel(self) -> None:
        if self.transformation_id is None:
            self._cognite_client.transformations.cancel(transformation_external_id=self.transformation_external_id)
        else:
            self._cognite_client.transformations.cancel(transformation_id=self.transformation_id)

    def metrics(self) -> TransformationJobMetricList:
        """`Get job metrics.`"""
        assert self.id is not None
        return self._cognite_client.transformations.jobs.list_metrics(self.id)

    def wait(self, polling_interval: float = 1, timeout: Optional[float] = None) -> TransformationJob:
        """`Waits for the job to finish.`

        Args:
            polling_interval (float): time (s) to wait between job status updates, default is one second.
            timeout (Optional[float]): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status.

        Returns:
            TransformationJob: self.

        Examples:
            run transformations 1 and 2 in parallel, and run 3 once they finish successfully:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> job1 = c.transformations.run(id = 1, wait = False)
                >>> job2 = c.transformations.run(id = 2, wait = False)
                >>> job1.wait()
                >>> job2.wait()
                >>> if TransformationJobStatus.FAILED not in [job1.status, job2.status]:
                >>>     c.transformations.run(id = 3, wait = False)

            wait transformation for 5 minutes and do something if still running:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> job = c.transformations.run(id = 1, wait = False)
                >>> job.wait(timeout = 5.0*60)
                >>> if job.status == TransformationJobStatus.FAILED:
                >>>     # do something if job failed
                >>> elif job.status == TransformationJobStatus.COMPLETED:
                >>>     # do something if job completed successfully
                >>> else:
                >>>     # do something if job is still running
        """
        self.update()
        if timeout is None:
            timeout = float("inf")
        waited = 0.0
        while waited < timeout and self.status not in [
            TransformationJobStatus.FAILED,
            TransformationJobStatus.COMPLETED,
        ]:
            to_wait = min(timeout - waited, polling_interval)
            time.sleep(to_wait)
            self.update()
            waited += polling_interval

        return self

    async def wait_async(self, polling_interval: float = 1, timeout: Optional[float] = None) -> TransformationJob:
        """Asyncio coroutine, waits for the job to finish asynchronously.

        Args:
            polling_interval (float): time (s) to wait between job status updates, default is one second.
            timeout (Optional[float]): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status.

        Returns:
            Awaitable[TransformationJob]: coroutine object that will finish when the job finishes and resolves to self.

        Examples:

            run transformations 1 and 2 in parallel, and run 3 once they finish successfully:

                >>> from asyncio import ensure_future
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> async def run_successive_transformations():
                >>>     job1 = c.transformations.run(id = 1, wait = False)
                >>>     job2 = c.transformations.run(id = 2, wait = False)
                >>>     await job1.wait_async()
                >>>     await job2.wait_async()
                >>>     if TransformationJobStatus.FAILED not in [job1.status, job2.status]:
                >>>         c.transformations.run(id = 3, wait = False)
                >>>
                >>> ensure_future(run_successive_transformations())

            wait transformation for 5 minutes and do something if still running:

                >>> from asyncio import ensure_future
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> async def run_successive_transformations():
                >>>     job = c.transformations.run(id = 1, wait = False)
                >>>     await job.wait_async(timeout = 5.0*60)
                >>>     if job.status == TransformationJobStatus.FAILED:
                >>>         # do something if job failed
                >>>     elif job.status == TransformationJobStatus.COMPLETED:
                >>>         # do something if job completed successfully
                >>>     else:
                >>>         # do something if job is still running
                >>>
                >>> ensure_future(run_successive_transformations())
        """
        self.update()
        if timeout is None:
            timeout = float("inf")
        waited = 0.0
        while waited < timeout and self.status not in [
            TransformationJobStatus.FAILED,
            TransformationJobStatus.COMPLETED,
        ]:
            to_wait = min(timeout - waited, polling_interval)
            await asyncio.sleep(to_wait)
            self.update()
            waited += polling_interval

        return self

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client: CogniteClient = None) -> TransformationJob:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.destination, Dict):
            instance.destination = _load_destination_dct(instance.destination)
        return instance

    def __hash__(self) -> int:
        return hash(self.id)


class TransformationJobList(CogniteResourceList):
    _RESOURCE = TransformationJob


class TransformationJobFilter(CogniteFilter):
    """

    Args:
        transformation_id (Optional[int]):  Filter jobs by transformation internal numeric ID.
        transformation_external_id (str): Filter jobs by transformation external ID.
    """

    def __init__(self, transformation_id: Optional[int] = None, transformation_external_id: str = None):
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
