import asyncio
import time
from enum import Enum
from typing import Dict, Optional

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

    async def wait_async(self, polling_interval: float = 1, timeout: Optional[float] = None):
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
