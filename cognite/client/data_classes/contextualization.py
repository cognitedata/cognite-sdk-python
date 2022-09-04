import time
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Type, TypeVar, Union, cast

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.exceptions import ModelFailedException
from cognite.client.utils._auxiliary import convert_true_match

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class JobStatus(Enum):
    QUEUED = "Queued"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    DISTRIBUTING = "Distributing"
    DISTRIBUTED = "Distributed"
    COLLECTING = "Collecting"

    def is_not_finished(self) -> bool:
        return self in [
            JobStatus.QUEUED,
            JobStatus.RUNNING,
            JobStatus.DISTRIBUTED,
            JobStatus.DISTRIBUTING,
            JobStatus.COLLECTING,
        ]


class ContextualizationJobType(Enum):
    ENTITY_MATCHING = "entity_matching"
    DIAGRAMS = "diagrams"


T_ContextualizationJob = TypeVar("T_ContextualizationJob", bound="ContextualizationJob")


class ContextualizationJob(CogniteResource):
    _COMMON_FIELDS = {
        "status",
        "jobId",
        "modelId",
        "pipelineId",
        "errorMessage",
        "createdTime",
        "startTime",
        "statusTime",
    }
    _JOB_TYPE = ContextualizationJobType.ENTITY_MATCHING

    def __init__(
        self,
        job_id: int = None,
        model_id: int = None,
        status: str = None,
        error_message: str = None,
        created_time: int = None,
        start_time: int = None,
        status_time: int = None,
        status_path: str = None,
        cognite_client: "CogniteClient" = None,
    ):
        """Data class for the result of a contextualization job."""
        self.job_id = job_id
        self.model_id = model_id
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.status_time = status_time
        self.error_message = error_message
        self._cognite_client = cast("CogniteClient", cognite_client)
        self._result: Optional[Dict[str, Any]] = None
        self._status_path = status_path

    def update_status(self) -> str:
        """Updates the model status and returns it"""
        data = (
            self._cognite_client.__getattribute__(self._JOB_TYPE.value)._get(f"{self._status_path}{self.job_id}").json()
        )
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        self._result = {k: v for k, v in data.items() if k not in self._COMMON_FIELDS}
        assert self.status is not None
        return self.status

    def wait_for_completion(self, timeout: int = None, interval: int = 1) -> None:
        """Waits for job completion, raising ModelFailedException if fit failed - generally not needed to call as it is called by result.
        Args:
            timeout (int): Time out after this many seconds. (None means wait indefinitely)
            interval (int): Poll status every this many seconds.
        """
        start = time.time()
        while timeout is None or time.time() < start + timeout:
            self.update_status()
            if not JobStatus(self.status).is_not_finished():
                break
            time.sleep(interval)
        if JobStatus(self.status) == JobStatus.FAILED:
            raise ModelFailedException(self.__class__.__name__, cast(int, self.job_id), cast(str, self.error_message))

    @property
    def result(self) -> Dict[str, Any]:
        """Waits for the job to finish and returns the results."""
        if not self._result:
            self.wait_for_completion()
        assert self._result is not None
        return self._result

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id: {self.job_id},status: {self.status},error: {self.error_message})"

    @classmethod
    def _load_with_status(
        cls: Type[T_ContextualizationJob], data: Dict[str, Any], status_path: str, cognite_client: Any
    ) -> T_ContextualizationJob:
        obj = cls._load(data, cognite_client=cognite_client)
        obj._status_path = status_path
        return obj


class ContextualizationJobList(CogniteResourceList):
    _RESOURCE = ContextualizationJob


class EntityMatchingModel(CogniteResource):
    _RESOURCE_PATH = "/context/entitymatching"
    _STATUS_PATH = _RESOURCE_PATH + "/"

    def __init__(
        self,
        id: int = None,
        status: str = None,
        error_message: str = None,
        created_time: int = None,
        start_time: int = None,
        status_time: int = None,
        classifier: str = None,
        feature_type: str = None,
        match_fields: List[str] = None,
        model_type: str = None,
        name: str = None,
        description: str = None,
        external_id: str = None,
        cognite_client: "CogniteClient" = None,
    ):
        """Entity matching model. See the `fit` method for the meaning of these fields."""
        self.id = id
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.status_time = status_time
        self.error_message = error_message
        self.classifier = classifier
        self.feature_type = feature_type
        self.match_fields = match_fields
        self.model_type = model_type
        self.name = name
        self.description = description
        self.external_id = external_id
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id: {self.id},status: {self.status},error: {self.error_message})"

    def update_status(self) -> str:
        """Updates the model status and returns it"""
        data = self._cognite_client.entity_matching._get(f"{self._STATUS_PATH}{self.id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        assert self.status is not None
        return self.status

    def wait_for_completion(self, timeout: int = None, interval: int = 1) -> None:
        """Waits for model completion, raising ModelFailedException if fit failed - generally not needed to call as it is called by predict

        Args:
            timeout: Time out after this many seconds. (None means wait indefinitely)
            interval: Poll status every this many seconds.
        """
        start = time.time()
        while timeout is None or time.time() < start + timeout:
            self.update_status()
            if JobStatus(self.status) not in [JobStatus.QUEUED, JobStatus.RUNNING]:
                break
            time.sleep(interval)
        if JobStatus(self.status) == JobStatus.FAILED:
            assert self.id is not None
            assert self.error_message is not None
            raise ModelFailedException(self.__class__.__name__, self.id, self.error_message)

    def predict(
        self,
        sources: Optional[List[Dict]] = None,
        targets: Optional[List[Dict]] = None,
        num_matches: int = 1,
        score_threshold: float = None,
    ) -> ContextualizationJob:
        """Predict entity matching. NB. blocks and waits for the model to be ready if it has been recently created.

        Args:
            sources: entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets: entities to match to, does not need an 'id' field.  Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float): only return matches with a score above this threshold
            ignore_missing_fields (bool): whether missing data in match_fields should be filled in with an empty string.

        Returns:
            ContextualizationJob: object which can be used to wait for and retrieve results."""
        self.wait_for_completion()
        return self._cognite_client.entity_matching._run_job(
            job_path="/predict",
            job_cls=ContextualizationJob,
            status_path="/jobs/",
            json={
                "id": self.id,
                "sources": self._dump_entities(sources),
                "targets": self._dump_entities(targets),
                "numMatches": num_matches,
                "scoreThreshold": score_threshold,
            },
        )

    def refit(
        self, true_matches: Sequence[Union[Dict, Tuple[Union[int, str], Union[int, str]]]]
    ) -> "EntityMatchingModel":
        """Re-fits an entity matching model, using the combination of the old and new true matches.

        Args:
            true_matches: Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId').
                A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
        Returns:
            EntityMatchingModel: new model refitted to true_matches."""
        true_matches = [convert_true_match(true_match) for true_match in true_matches]
        self.wait_for_completion()
        response = self._cognite_client.entity_matching._post(
            self._RESOURCE_PATH + "/refit", json={"trueMatches": true_matches, "id": self.id}
        )
        return self._load(response.json(), cognite_client=self._cognite_client)

    @staticmethod
    def _flatten_entity(entity: Union[dict, CogniteResource]) -> Dict:
        if isinstance(entity, CogniteResource):
            entity = entity.dump(camel_case=True)
        if "metadata" in entity and isinstance(entity["metadata"], dict):
            for k, v in entity["metadata"].items():
                entity["metadata.{}".format(k)] = v
        return {k: v for k, v in entity.items() if k == "id" or isinstance(v, str)}

    @staticmethod
    def _dump_entities(entities: Optional[Sequence[Union[Dict, CogniteResource]]]) -> Optional[List[Dict]]:
        if entities:
            return [EntityMatchingModel._flatten_entity(e) for e in entities]
        return None


class EntityMatchingModelUpdate(CogniteUpdate):
    """Changes applied to entity matching model

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "EntityMatchingModelUpdate":
            return self._set(value)

    @property
    def name(self) -> _PrimitiveUpdate:
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveUpdate:
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "description")


class EntityMatchingModelList(CogniteResourceList):
    _RESOURCE = EntityMatchingModel


class DiagramConvertPage(CogniteResource):
    def __init__(
        self, page: int = None, png_url: str = None, svg_url: str = None, cognite_client: "CogniteClient" = None
    ):
        self.page = page
        self.png_url = png_url
        self.svg_url = svg_url
        self._cognite_client = cast("CogniteClient", cognite_client)


class DiagramConvertPageList(CogniteResourceList):
    _RESOURCE = DiagramConvertPage


class DiagramConvertItem(CogniteResource):
    def __init__(
        self,
        file_id: int = None,
        file_external_id: str = None,
        results: list = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.results = results
        self._cognite_client = cast("CogniteClient", cognite_client)

    def __len__(self) -> int:
        assert self.results
        return len(self.results)

    @property
    def pages(self) -> DiagramConvertPageList:
        assert self.results is not None
        return DiagramConvertPageList._load(self.results, cognite_client=self._cognite_client)

    def to_pandas(self, camel_case: bool = False) -> "pandas.DataFrame":  # type: ignore[override]
        df = super().to_pandas(camel_case=camel_case)
        df.loc["results"] = f"{len(df['results'])} pages"
        return df


class DiagramConvertResults(ContextualizationJob):
    _JOB_TYPE = ContextualizationJobType.DIAGRAMS

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._items: Optional[list] = None

    def __getitem__(self, find_id: Any) -> DiagramConvertItem:
        """retrieves the results for the file with (external) id"""
        found = [
            item
            for item in self.result["items"]
            if item.get("fileId") == find_id or item.get("fileExternalId") == find_id
        ]
        if not found:
            raise IndexError(f"File with (external) id {find_id} not found in results")
        if len(found) != 1:
            raise IndexError(f"Found multiple results for file with (external) id {find_id}, use .items instead")
        return DiagramConvertItem._load(found[0], cognite_client=self._cognite_client)

    @property
    def items(self) -> Optional[List[DiagramConvertItem]]:
        """returns a list of all results by file"""
        if self.status == "Completed":
            self._items = [
                DiagramConvertItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items: list) -> None:
        self._items = items


class DiagramDetectItem(CogniteResource):
    def __init__(
        self,
        file_id: int = None,
        file_external_id: str = None,
        annotations: list = None,
        error_message: str = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.annotations = annotations
        self.error_message = error_message
        self._cognite_client = cast("CogniteClient", cognite_client)

    def to_pandas(self, camel_case: bool = False) -> "pandas.DataFrame":  # type: ignore[override]
        df = super().to_pandas(camel_case=camel_case)
        df.loc["annotations"] = f"{len(df['annotations'])} annotations"
        return df


class DiagramDetectResults(ContextualizationJob):
    _JOB_TYPE = ContextualizationJobType.DIAGRAMS

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._items: Optional[List[DiagramDetectItem]] = None

    def __getitem__(self, find_id: Any) -> DiagramDetectItem:
        """retrieves the results for the file with (external) id"""
        found = [
            item
            for item in self.result["items"]
            if item.get("fileId") == find_id or item.get("fileExternalId") == find_id
        ]
        if not found:
            raise IndexError(f"File with (external) id {find_id} not found in results")
        if len(found) != 1:
            raise IndexError(f"Found multiple results for file with (external) id {find_id}, use .items instead")
        return DiagramDetectItem._load(found[0], cognite_client=self._cognite_client)

    @property
    def items(self) -> Optional[List[DiagramDetectItem]]:
        """returns a list of all results by file"""
        if self.status == "Completed":
            self._items = [
                DiagramDetectItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items: List[DiagramDetectItem]) -> None:
        self._items = items

    @property
    def errors(self) -> List[str]:
        """returns a list of all error messages across files"""
        return [item["errorMessage"] for item in self.result["items"] if "errorMessage" in item]

    def convert(self) -> DiagramConvertResults:
        """Convert a P&ID to an interactive SVG where the provided annotations are highlighted"""
        return self._cognite_client.diagrams.convert(detect_job=self)
