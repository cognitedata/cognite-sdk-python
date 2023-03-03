
import time
from numbers import Number
from cognite.client._constants import LIST_LIMIT_CEILING, LIST_LIMIT_DEFAULT
from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteResourceList, CogniteResponse
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._auxiliary import is_unlimited
if TYPE_CHECKING:
    from cognite.client import CogniteClient

class Function(CogniteResource):

    def __init__(self, id=None, name=None, external_id=None, description=None, owner=None, status=None, file_id=None, function_path=None, created_time=None, api_key=None, secrets=None, env_vars=None, cpu=None, memory=None, runtime=None, runtime_version=None, metadata=None, error=None, cognite_client=None):
        self.id = cast(int, id)
        self.name = cast(str, name)
        self.external_id = external_id
        self.description = description
        self.owner = owner
        self.status = status
        self.file_id = file_id
        self.function_path = function_path
        self.created_time = created_time
        self.api_key = api_key
        self.secrets = secrets
        self.env_vars = env_vars
        self.cpu = cpu
        self.memory = memory
        self.runtime = runtime
        self.runtime_version = runtime_version
        self.metadata = metadata
        self.error = error
        self._cognite_client = cast('CogniteClient', cognite_client)

    def call(self, data=None, wait=True):
        return self._cognite_client.functions.call(id=self.id, data=data, wait=wait)

    def list_calls(self, status=None, schedule_id=None, start_time=None, end_time=None, limit=LIST_LIMIT_DEFAULT):
        return self._cognite_client.functions.calls.list(function_id=self.id, status=status, schedule_id=schedule_id, start_time=start_time, end_time=end_time, limit=limit)

    def list_schedules(self, limit=LIST_LIMIT_DEFAULT):
        schedules_by_external_id = self._cognite_client.functions.schedules.list(function_external_id=self.external_id, limit=limit)
        schedules_by_id = self._cognite_client.functions.schedules.list(function_id=self.id, limit=limit)
        if is_unlimited(limit):
            limit = LIST_LIMIT_CEILING
        return (schedules_by_external_id + schedules_by_id)[:limit]

    def retrieve_call(self, id):
        return self._cognite_client.functions.calls.retrieve(call_id=id, function_id=self.id)

    def update(self):
        latest = self._cognite_client.functions.retrieve(id=self.id)
        if (latest is None):
            return None
        for attribute in self.__dict__:
            if attribute.startswith('_'):
                continue
            latest_value = getattr(latest, attribute)
            setattr(self, attribute, latest_value)

class FunctionFilter(CogniteFilter):

    def __init__(self, name=None, owner=None, file_id=None, status=None, external_id_prefix=None, created_time=None):
        self.name = name
        self.owner = owner
        self.file_id = file_id
        self.status = status
        self.external_id_prefix = external_id_prefix
        self.created_time = created_time

class FunctionCallsFilter(CogniteFilter):

    def __init__(self, status=None, schedule_id=None, start_time=None, end_time=None):
        self.status = status
        self.schedule_id = schedule_id
        self.start_time = start_time
        self.end_time = end_time

class FunctionSchedule(CogniteResource):

    def __init__(self, id=None, name=None, function_id=None, function_external_id=None, description=None, created_time=None, cron_expression=None, session_id=None, cognite_client=None):
        self.id = id
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.description = description
        self.cron_expression = cron_expression
        self.created_time = created_time
        self.session_id = session_id
        self._cognite_client = cast('CogniteClient', cognite_client)

    def get_input_data(self):
        return self._cognite_client.functions.schedules.get_input_data(id=self.id)

class FunctionSchedulesFilter(CogniteFilter):

    def __init__(self, name=None, function_id=None, function_external_id=None, created_time=None, cron_expression=None):
        self.name = name
        self.function_id = function_id
        self.function_external_id = function_external_id
        self.created_time = created_time
        self.cron_expression = cron_expression

class FunctionSchedulesList(CogniteResourceList):
    _RESOURCE = FunctionSchedule

class FunctionList(CogniteResourceList):
    _RESOURCE = Function

class FunctionCall(CogniteResource):

    def __init__(self, id=None, start_time=None, end_time=None, scheduled_time=None, status=None, schedule_id=None, error=None, function_id=None, cognite_client=None):
        self.id = id
        self.start_time = start_time
        self.end_time = end_time
        self.scheduled_time = scheduled_time
        self.status = status
        self.schedule_id = schedule_id
        self.error = error
        self.function_id = function_id
        self._cognite_client = cast('CogniteClient', cognite_client)

    def get_response(self):
        return self._cognite_client.functions.calls.get_response(call_id=self.id, function_id=self.function_id)

    def get_logs(self):
        return self._cognite_client.functions.calls.get_logs(call_id=self.id, function_id=self.function_id)

    def update(self):
        latest = self._cognite_client.functions.calls.retrieve(call_id=self.id, function_id=self.function_id)
        self.status = latest.status
        self.end_time = latest.end_time
        self.error = latest.error

    def wait(self):
        while (self.status == 'Running'):
            self.update()
            time.sleep(1.0)

class FunctionCallList(CogniteResourceList):
    _RESOURCE = FunctionCall

class FunctionCallLogEntry(CogniteResource):

    def __init__(self, timestamp=None, message=None, cognite_client=None):
        self.timestamp = timestamp
        self.message = message
        self._cognite_client = cast('CogniteClient', cognite_client)

class FunctionCallLog(CogniteResourceList):
    _RESOURCE = FunctionCallLogEntry

class FunctionsLimits(CogniteResponse):

    def __init__(self, timeout_minutes, cpu_cores, memory_gb, runtimes, response_size_mb=None):
        self.timeout_minutes = timeout_minutes
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.runtimes = runtimes
        self.response_size_mb = response_size_mb

    @classmethod
    def _load(cls, api_response):
        return cls(timeout_minutes=api_response['timeoutMinutes'], cpu_cores=api_response['cpuCores'], memory_gb=api_response['memoryGb'], runtimes=api_response['runtimes'], response_size_mb=api_response.get('responseSizeMb'))

class FunctionsStatus(CogniteResponse):

    def __init__(self, status):
        self.status = status

    @classmethod
    def _load(cls, api_response):
        return cls(status=api_response['status'])
