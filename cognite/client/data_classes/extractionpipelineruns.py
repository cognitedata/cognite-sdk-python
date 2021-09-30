from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


class ExtractionPipelineRun(CogniteResource):
    """A representation of an extraction pipeline run.

    Args:
        external_id (str): The external ID of the extraction pipeline.
        status (str): success/failure/seen.
        message (str): Optional status message.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        status: str = None,
        message: str = None,
        created_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.status = status
        self.message = message
        self.created_time = created_time
        self._cognite_client = cognite_client


class ExtractionPipelineRunUpdate(CogniteUpdate):
    class _PrimitiveExtractionPipelineRunUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "ExtractionPipelineRunUpdate":
            return self._set(value)


class ExtractionPipelineRunList(CogniteResourceList):
    _RESOURCE = ExtractionPipelineRun
    _UPDATE = ExtractionPipelineRunUpdate


class StringFilter(CogniteFilter):
    """Filter runs on substrings of the message

    Args:
        substring (str): Part of message
    """

    def __init__(self, substring: str = None):
        self.substring = substring


class ExtractionPipelineRunFilter(CogniteFilter):
    """Filter runs with exact matching

    Args:
        external_id (str): The external ID of related ExtractionPipeline provided by the client. Must be unique for the resource type.
        statuses (List[str]): success/failure/seen.
        message (StringFilter): message filter.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        statuses: List[str] = None,
        message: StringFilter = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.statuses = statuses
        self.message = message
        self.created_time = created_time
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(ExtractionPipelineRunFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
        return instance
