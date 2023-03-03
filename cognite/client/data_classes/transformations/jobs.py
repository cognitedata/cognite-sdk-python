import asyncio
import time
from enum import Enum

from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteResourceList
from cognite.client.data_classes.transformations.common import _load_destination_dct


class TransformationJobStatus(str, Enum):
    RUNNING = "Running"
    CREATED = "Created"
    COMPLETED = "Completed"
    FAILED = "Failed"


class TransformationJobMetric(CogniteResource):
    def __init__(self, id=None, timestamp=None, name=None, count=None, cognite_client=None):
        self.timestamp = timestamp
        self.name = name
        self.count = count
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        return instance


class TransformationJobMetricList(CogniteResourceList):
    _RESOURCE = TransformationJobMetric


class TransformationJob(CogniteResource):
    def __init__(
        self,
        id=None,
        status=None,
        transformation_id=None,
        transformation_external_id=None,
        source_project=None,
        destination_project=None,
        destination=None,
        conflict_mode=None,
        query=None,
        error=None,
        ignore_null_fields=False,
        created_time=None,
        started_time=None,
        finished_time=None,
        last_seen_time=None,
        cognite_client=None,
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
        self._cognite_client = cognite_client

    def update(self):
        updated = self._cognite_client.transformations.jobs.retrieve(id=self.id)
        self.status = updated.status
        self.error = updated.error
        self.started_time = updated.started_time
        self.finished_time = updated.finished_time
        self.last_seen_time = updated.last_seen_time

    def cancel(self):
        if self.transformation_id is None:
            self._cognite_client.transformations.cancel(transformation_external_id=self.transformation_external_id)
        else:
            self._cognite_client.transformations.cancel(transformation_id=self.transformation_id)

    def metrics(self):
        assert self.id is not None
        return self._cognite_client.transformations.jobs.list_metrics(self.id)

    def wait(self, polling_interval=1, timeout=None):
        self.update()
        if timeout is None:
            timeout = float("inf")
        waited = 0.0
        while (waited < timeout) and (
            self.status not in [TransformationJobStatus.FAILED, TransformationJobStatus.COMPLETED]
        ):
            to_wait = min((timeout - waited), polling_interval)
            time.sleep(to_wait)
            self.update()
            waited += polling_interval
        return self

    async def wait_async(self, polling_interval: float = 1, timeout: Optional[float] = None) -> TransformationJob:
        "Asyncio coroutine, waits for the job to finish asynchronously.\n\n        Args:\n            polling_interval (float): time (s) to wait between job status updates, default is one second.\n            timeout (Optional[float]): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status.\n\n        Returns:\n            Awaitable[TransformationJob]: coroutine object that will finish when the job finishes and resolves to self.\n\n        Examples:\n\n            run transformations 1 and 2 in parallel, and run 3 once they finish successfully:\n\n                >>> from asyncio import ensure_future\n                >>> from cognite.client import CogniteClient\n                >>> c = CogniteClient()\n                >>>\n                >>> async def run_successive_transformations():\n                >>>     job1 = c.transformations.run(id = 1, wait = False)\n                >>>     job2 = c.transformations.run(id = 2, wait = False)\n                >>>     await job1.wait_async()\n                >>>     await job2.wait_async()\n                >>>     if TransformationJobStatus.FAILED not in [job1.status, job2.status]:\n                >>>         c.transformations.run(id = 3, wait = False)\n                >>>\n                >>> ensure_future(run_successive_transformations())\n\n            wait transformation for 5 minutes and do something if still running:\n\n                >>> from asyncio import ensure_future\n                >>> from cognite.client import CogniteClient\n                >>> c = CogniteClient()\n                >>>\n                >>> async def run_successive_transformations():\n                >>>     job = c.transformations.run(id = 1, wait = False)\n                >>>     await job.wait_async(timeout = 5.0*60)\n                >>>     if job.status == TransformationJobStatus.FAILED:\n                >>>         # do something if job failed\n                >>>     elif job.status == TransformationJobStatus.COMPLETED:\n                >>>         # do something if job completed successfully\n                >>>     else:\n                >>>         # do something if job is still running\n                >>>\n                >>> ensure_future(run_successive_transformations())\n"
        self.update()
        if timeout is None:
            timeout = float("inf")
        waited = 0.0
        while (waited < timeout) and (
            self.status not in [TransformationJobStatus.FAILED, TransformationJobStatus.COMPLETED]
        ):
            to_wait = min((timeout - waited), polling_interval)
            (await asyncio.sleep(to_wait))
            self.update()
            waited += polling_interval
        return self

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.destination, Dict):
            instance.destination = _load_destination_dct(instance.destination)
        return instance

    def __hash__(self):
        return hash(self.id)


class TransformationJobList(CogniteResourceList):
    _RESOURCE = TransformationJob


class TransformationJobFilter(CogniteFilter):
    def __init__(self, transformation_id=None, transformation_external_id=None):
        self.transformation_id = transformation_id
        self.transformation_external_id = transformation_external_id
