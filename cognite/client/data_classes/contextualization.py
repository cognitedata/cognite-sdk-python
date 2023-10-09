from __future__ import annotations

import time
import warnings
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Sequence, Type, TypeVar, Union, cast

from requests.utils import CaseInsensitiveDict

from cognite.client.data_classes import Annotation
from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
)
from cognite.client.data_classes.annotation_types.images import (
    AssetLink,
    KeypointCollectionWithObjectDetection,
    ObjectDetection,
    TextRegion,
)
from cognite.client.data_classes.annotation_types.primitives import VisionResource
from cognite.client.data_classes.annotations import AnnotationList
from cognite.client.exceptions import CogniteAPIError, CogniteException, ModelFailedException
from cognite.client.utils._auxiliary import convert_true_match, exactly_one_is_not_none
from cognite.client.utils._text import to_snake_case

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class JobStatus(Enum):
    _NOT_STARTED: None = None
    QUEUED = "Queued"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    DISTRIBUTING = "Distributing"
    DISTRIBUTED = "Distributed"
    COLLECTING = "Collecting"

    def is_finished(self) -> bool:
        return self in {JobStatus.COMPLETED, JobStatus.FAILED}

    def is_not_finished(self) -> bool:
        return self in {
            JobStatus._NOT_STARTED,
            JobStatus.QUEUED,
            JobStatus.RUNNING,
            JobStatus.DISTRIBUTED,
            JobStatus.DISTRIBUTING,
            JobStatus.COLLECTING,
        }


class ContextualizationJobType(Enum):
    ENTITY_MATCHING = "entity_matching"
    DIAGRAMS = "diagrams"
    VISION = "vision"


class ContextualizationJob(CogniteResource):
    """Data class for the result of a contextualization job."""

    _COMMON_FIELDS = frozenset(
        {
            "status",
            "jobId",
            "modelId",
            "pipelineId",
            "errorMessage",
            "createdTime",
            "startTime",
            "statusTime",
            "jobToken",
        }
    )
    _JOB_TYPE = ContextualizationJobType.ENTITY_MATCHING

    def __init__(
        self,
        job_id: int | None = None,
        model_id: int | None = None,
        status: str | None = None,
        error_message: str | None = None,
        created_time: int | None = None,
        start_time: int | None = None,
        status_time: int | None = None,
        status_path: str | None = None,
        job_token: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.job_id = job_id
        self.model_id = model_id
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.status_time = status_time
        self.error_message = error_message
        self.job_token = job_token
        self._cognite_client = cast("CogniteClient", cognite_client)
        self._result: dict[str, Any] | None = None
        self._status_path = status_path

    def update_status(self) -> str:
        """Updates the model status and returns it"""
        headers = {"X-Job-Token": self.job_token} if self.job_token else {}
        data = (
            getattr(self._cognite_client, self._JOB_TYPE.value)
            ._get(f"{self._status_path}{self.job_id}", headers=headers)
            .json()
        )
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        self._result = {k: v for k, v in data.items() if k not in self._COMMON_FIELDS}
        assert self.status is not None
        return self.status

    def wait_for_completion(self, timeout: int | None = None, interval: int = 1) -> None:
        """Waits for job completion. This is generally not needed to call directly, as `.result` will do so automatically.

        Args:
            timeout (int | None): Time out after this many seconds. (None means wait indefinitely)
            interval (int): Poll status every this many seconds.

        Raises:
            ModelFailedException: The model fit failed.
        """
        start = time.time()
        while timeout is None or time.time() < start + timeout:
            self.update_status()
            if JobStatus(self.status).is_finished():
                break
            time.sleep(interval)
        if JobStatus(self.status) is JobStatus.FAILED:
            raise ModelFailedException(self.__class__.__name__, cast(int, self.job_id), cast(str, self.error_message))

    @property
    def result(self) -> dict[str, Any]:
        """Waits for the job to finish and returns the results."""
        if not self._result:
            self.wait_for_completion()
        assert self._result is not None
        return self._result

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.job_id}, status={self.status}, error={self.error_message})"

    @classmethod
    def _load_with_status(
        cls: type[T_ContextualizationJob],
        *,
        data: dict[str, Any],
        headers: CaseInsensitiveDict[str],
        status_path: str,
        cognite_client: Any,
    ) -> T_ContextualizationJob:
        obj = cls._load({**data, "jobToken": headers.get("X-Job-Token")}, cognite_client=cognite_client)
        obj._status_path = status_path
        # '_load' does not see properties (real attribute stored under a different name, e.g. '_items' not 'items'):
        if "items" in data and hasattr(obj, "items"):
            obj.items = data["items"]
        return obj


T_ContextualizationJob = TypeVar("T_ContextualizationJob", bound=ContextualizationJob)


class ContextualizationJobList(CogniteResourceList[ContextualizationJob]):
    _RESOURCE = ContextualizationJob


class EntityMatchingModel(CogniteResource):
    """Entity matching model. See the `fit` method for the meaning of these fields."""

    _RESOURCE_PATH = "/context/entitymatching"
    _STATUS_PATH = _RESOURCE_PATH + "/"

    def __init__(
        self,
        id: int | None = None,
        status: str | None = None,
        error_message: str | None = None,
        created_time: int | None = None,
        start_time: int | None = None,
        status_time: int | None = None,
        classifier: str | None = None,
        feature_type: str | None = None,
        match_fields: list[str] | None = None,
        model_type: str | None = None,
        name: str | None = None,
        description: str | None = None,
        external_id: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
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
        return f"{self.__class__.__name__}(id={self.id}, status={self.status}, error={self.error_message})"

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

    def wait_for_completion(self, timeout: int | None = None, interval: int = 1) -> None:
        """Waits for model completion. This is generally not needed to call directly, as `.result` will do so automatically.

        Args:
            timeout (int | None): Time out after this many seconds. (None means wait indefinitely)
            interval (int): Poll status every this many seconds.

        Raises:
            ModelFailedException: The model fit failed.
        """
        start = time.time()
        while timeout is None or time.time() < start + timeout:
            self.update_status()
            if JobStatus(self.status) not in [JobStatus.QUEUED, JobStatus.RUNNING]:
                break
            time.sleep(interval)
        if JobStatus(self.status) is JobStatus.FAILED:
            assert self.id is not None
            assert self.error_message is not None
            raise ModelFailedException(self.__class__.__name__, self.id, self.error_message)

    def predict(
        self,
        sources: list[dict] | None = None,
        targets: list[dict] | None = None,
        num_matches: int = 1,
        score_threshold: float | None = None,
    ) -> ContextualizationJob:
        """Predict entity matching. NB. blocks and waits for the model to be ready if it has been recently created.

        Args:
            sources (list[dict] | None): entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets (list[dict] | None): entities to match to, does not need an 'id' field.  Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float | None): only return matches with a score above this threshold

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

    def refit(self, true_matches: Sequence[dict | tuple[int | str, int | str]]) -> EntityMatchingModel:
        """Re-fits an entity matching model, using the combination of the old and new true matches.

        Args:
            true_matches (Sequence[dict | tuple[int | str, int | str]]): Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId'). A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
        Returns:
            EntityMatchingModel: new model refitted to true_matches."""
        true_matches = [convert_true_match(true_match) for true_match in true_matches]
        self.wait_for_completion()
        response = self._cognite_client.entity_matching._post(
            self._RESOURCE_PATH + "/refit", json={"trueMatches": true_matches, "id": self.id}
        )
        return self._load(response.json(), cognite_client=self._cognite_client)

    @staticmethod
    def _flatten_entity(entity: dict | CogniteResource) -> dict:
        if isinstance(entity, CogniteResource):
            entity = entity.dump(camel_case=True)
        if "metadata" in entity and isinstance(entity["metadata"], dict):
            for k, v in entity["metadata"].items():
                entity[f"metadata.{k}"] = v
        return {k: v for k, v in entity.items() if k == "id" or isinstance(v, str)}

    @staticmethod
    def _dump_entities(entities: Sequence[dict | CogniteResource] | None) -> list[dict] | None:
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
        def set(self, value: Any) -> EntityMatchingModelUpdate:
            return self._set(value)

    @property
    def name(self) -> _PrimitiveUpdate:
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveUpdate:
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "description")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
        ]


class EntityMatchingModelList(CogniteResourceList[EntityMatchingModel]):
    _RESOURCE = EntityMatchingModel


class FileReference:
    def __init__(
        self,
        file_id: int | None = None,
        file_external_id: str | None = None,
        first_page: int | None = None,
        last_page: int | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.first_page = first_page
        self.last_page = last_page

        if not exactly_one_is_not_none(file_id, file_external_id):
            raise ValueError("Exactly one of file_id and file_external_id must be set for a file reference")
        if exactly_one_is_not_none(first_page, last_page):
            raise ValueError("If the page range feature is used, both first page and last page must be set")

    def to_api_item(self) -> dict[str, str | int | dict[str, int]]:
        if self.file_id is None and self.file_external_id is not None:
            item: dict[str, str | int | dict[str, int]] = {"fileExternalId": self.file_external_id}
        if self.file_id is not None and self.file_external_id is None:
            item = {"fileId": self.file_id}
        if self.first_page is not None and self.last_page is not None:
            item["pageRange"] = {"begin": self.first_page, "end": self.last_page}
        return item


class DiagramConvertPage(CogniteResource):
    def __init__(
        self,
        page: int | None = None,
        png_url: str | None = None,
        svg_url: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.page = page
        self.png_url = png_url
        self.svg_url = svg_url
        self._cognite_client = cast("CogniteClient", cognite_client)


class DiagramConvertPageList(CogniteResourceList[DiagramConvertPage]):
    _RESOURCE = DiagramConvertPage


class DiagramConvertItem(CogniteResource):
    def __init__(
        self,
        file_id: int | None = None,
        file_external_id: str | None = None,
        results: list | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
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

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        df = super().to_pandas(camel_case=camel_case)
        df.loc["results"] = f"{len(df['results'])} pages"
        return df


class DiagramConvertResults(ContextualizationJob):
    _JOB_TYPE = ContextualizationJobType.DIAGRAMS

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._items: list | None = None

    def __getitem__(self, find_id: Any) -> DiagramConvertItem:
        """Retrieves the results for the file with (external) id"""
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
    def items(self) -> list[DiagramConvertItem] | None:
        """returns a list of all results by file"""
        if JobStatus(self.status) is JobStatus.COMPLETED:
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
        file_id: int | None = None,
        file_external_id: str | None = None,
        annotations: list | None = None,
        error_message: str | None = None,
        cognite_client: CogniteClient | None = None,
        page_range: dict[str, int] | None = None,
        page_count: int | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.annotations = annotations
        self.error_message = error_message
        self._cognite_client = cast("CogniteClient", cognite_client)
        self.page_range = page_range
        self.page_count = page_count

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        df = super().to_pandas(camel_case=camel_case)
        df.loc["annotations"] = f"{len(df['annotations'])} annotations"
        return df


class DiagramDetectResults(ContextualizationJob):
    _JOB_TYPE = ContextualizationJobType.DIAGRAMS

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._items: list[DiagramDetectItem] | None = None

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
    def items(self) -> list[DiagramDetectItem] | None:
        """Returns a list of all results by file"""
        if JobStatus(self.status) is JobStatus.COMPLETED:
            self._items = [
                DiagramDetectItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items: list[DiagramDetectItem]) -> None:
        self._items = items

    @property
    def errors(self) -> list[str]:
        """Returns a list of all error messages across files"""
        return [item["errorMessage"] for item in self.result["items"] if "errorMessage" in item]

    def convert(self) -> DiagramConvertResults:
        """Convert a P&ID to an interactive SVG where the provided annotations are highlighted"""
        return self._cognite_client.diagrams.convert(detect_job=self)


# Vision dataclasses
FeatureClass = Union[Type[TextRegion], Type[AssetLink], Type[ObjectDetection]]


class VisionFeature(str, Enum):
    TEXT_DETECTION = "TextDetection"
    ASSET_TAG_DETECTION = "AssetTagDetection"
    PEOPLE_DETECTION = "PeopleDetection"
    # The features below are in beta
    INDUSTRIAL_OBJECT_DETECTION = "IndustrialObjectDetection"
    PERSONAL_PROTECTIVE_EQUIPMENT_DETECTION = "PersonalProtectiveEquipmentDetection"
    DIAL_GAUGE_DETECTION = "DialGaugeDetection"
    LEVEL_GAUGE_DETECTION = "LevelGaugeDetection"
    DIGITAL_GAUGE_DETECTION = "DigitalGaugeDetection"
    VALVE_DETECTION = "ValveDetection"

    @classmethod
    def beta_features(cls) -> set[VisionFeature]:
        """Returns a set of VisionFeature's that are currently in beta"""
        return {cls.INDUSTRIAL_OBJECT_DETECTION, cls.PERSONAL_PROTECTIVE_EQUIPMENT_DETECTION}


@dataclass
class VisionExtractPredictions(VisionResource):
    text_predictions: list[TextRegion] | None = None
    asset_tag_predictions: list[AssetLink] | None = None
    industrial_object_predictions: list[ObjectDetection] | None = None
    people_predictions: list[ObjectDetection] | None = None
    personal_protective_equipment_predictions: list[ObjectDetection] | None = None
    digital_gauge_predictions: list[ObjectDetection] | None = None
    dial_gauge_predictions: list[KeypointCollectionWithObjectDetection] | None = None
    level_gauge_predictions: list[KeypointCollectionWithObjectDetection] | None = None
    valve_predictions: list[KeypointCollectionWithObjectDetection] | None = None


VISION_FEATURE_MAP: dict[str, Any] = {
    "text_predictions": TextRegion,
    "asset_tag_predictions": AssetLink,
    "industrial_object_predictions": ObjectDetection,
    "people_predictions": ObjectDetection,
    "personal_protective_equipment_predictions": ObjectDetection,
    "digital_gauge_predictions": ObjectDetection,
    "dial_gauge_predictions": KeypointCollectionWithObjectDetection,
    "level_gauge_predictions": KeypointCollectionWithObjectDetection,
    "valve_predictions": KeypointCollectionWithObjectDetection,
}


VISION_ANNOTATION_TYPE_MAP: dict[str, str | dict[str, str]] = {
    "text_predictions": "images.TextRegion",
    "asset_tag_predictions": "images.AssetLink",
    "industrial_object_predictions": "images.ObjectDetection",
    "people_predictions": "images.ObjectDetection",
    "personal_protective_equipment_predictions": "images.ObjectDetection",
    "digital_gauge_predictions": "images.ObjectDetection",
    "dial_gauge_predictions": {
        "keypoint_collection": "images.KeypointCollection",
        "object_detection": "images.ObjectDetection",
    },
    "level_gauge_predictions": {
        "keypoint_collection": "images.KeypointCollection",
        "object_detection": "images.ObjectDetection",
    },
    "valve_predictions": {
        "keypoint_collection": "images.KeypointCollection",
        "object_detection": "images.ObjectDetection",
    },
}


@dataclass
class ThresholdParameter:
    threshold: float | None = None


@dataclass
class AssetTagDetectionParameters(VisionResource, ThresholdParameter):
    partial_match: bool | None = None
    asset_subtree_ids: list[int] | None = None


@dataclass
class TextDetectionParameters(VisionResource, ThresholdParameter):
    pass


@dataclass
class PeopleDetectionParameters(VisionResource, ThresholdParameter):
    pass


@dataclass
class IndustrialObjectDetectionParameters(VisionResource, ThresholdParameter):
    pass


@dataclass
class PersonalProtectiveEquipmentDetectionParameters(VisionResource, ThresholdParameter):
    pass


@dataclass
class DialGaugeDetection(VisionResource, ThresholdParameter):
    min_level: float | None = None
    max_level: float | None = None
    dead_angle: float | None = None
    non_linear_angle: float | None = None


@dataclass
class LevelGaugeDetection(VisionResource, ThresholdParameter):
    min_level: float | None = None
    max_level: float | None = None


@dataclass
class DigitalGaugeDetection(VisionResource, ThresholdParameter):
    comma_position: int | None = None
    min_num_digits: int | None = None
    max_num_digits: int | None = None


@dataclass
class ValveDetection(VisionResource, ThresholdParameter):
    pass


class DetectJobBundle:
    _RESOURCE_PATH = "/context/diagram/detect/"
    _STATUS_PATH = "/context/diagram/detect/status"
    _WAIT_TIME = 2

    def __init__(self, job_ids: list[int], cognite_client: CogniteClient | None = None) -> None:
        warnings.warn(
            "DetectJobBundle.result is calling a beta endpoint which is still in development. "
            "Breaking changes can happen in between patch versions."
        )

        self._cognite_client = cast("CogniteClient", cognite_client)
        if not job_ids:
            raise ValueError("You need to specify job_ids")
        self.job_ids = job_ids
        self._remaining_job_ids: list[int] = []

        self.jobs: list[dict[str, Any]] = []

        self._result: list[dict[str, Any]] = []

    def __str__(self) -> str:
        return f"DetectJobBundle({self.job_ids=}, {self.jobs=}, {self._result=}, {self._remaining_job_ids=})"

    def _back_off(self) -> None:
        """
        Linear back off, in order to limit load on our API.
        Starts at _WAIT_TIME and goes to 10 seconds.
        """
        time.sleep(self._WAIT_TIME)
        if self._WAIT_TIME < 10:
            self._WAIT_TIME += 2

    def wait_for_completion(self, timeout: int | None = None) -> None:
        """Waits for all jobs to complete, generally not needed to call as it is called by result.

        Args:
            timeout (int | None): Time out after this many seconds. (None means wait indefinitely)
        """
        start = time.time()
        self._remaining_job_ids = self.job_ids
        while timeout is None or time.time() < start + timeout:
            try:
                res = self._cognite_client.diagrams._post(self._STATUS_PATH, json={"items": self._remaining_job_ids})
            except CogniteAPIError:
                self._back_off()
                continue
            if res.json().get("error"):
                break
            self.jobs = res.json()["items"]

            # Assign the jobs that aren't finished
            self._remaining_job_ids = [j["jobId"] for j in self.jobs if JobStatus(j["status"]).is_not_finished()]

            if self._remaining_job_ids:
                self._back_off()
            else:
                self._WAIT_TIME = 2
                break

    def fetch_results(self) -> list[dict[str, Any]]:
        return [self._cognite_client.diagrams._get(f"{self._RESOURCE_PATH}{j}").json() for j in self.job_ids]

    @property
    def result(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Waits for the job to finish and returns the results."""
        if not self._result:
            self.wait_for_completion()

            self._result = self.fetch_results()
        assert self._result is not None
        # Sort into succeeded and failed
        failed: list[dict[str, Any]] = []
        succeeded: list[dict[str, Any]] = []
        for job_result in self._result:
            for item in job_result["items"]:
                if "errorMessage" in item:
                    failed.append({**item, **{"job_id": job_result["jobId"], "status": job_result["status"]}})
                else:
                    succeeded.append({**item, **{"job_id": job_result["jobId"], "status": job_result["status"]}})
        return succeeded, failed


@dataclass
class FeatureParameters(VisionResource):
    text_detection_parameters: TextDetectionParameters | None = None
    asset_tag_detection_parameters: AssetTagDetectionParameters | None = None
    people_detection_parameters: PeopleDetectionParameters | None = None
    industrial_object_detection_parameters: IndustrialObjectDetectionParameters | None = None
    personal_protective_equipment_detection_parameters: PersonalProtectiveEquipmentDetectionParameters | None = None
    digital_gauge_detection_parameters: DigitalGaugeDetection | None = None
    dial_gauge_detection_parameters: DialGaugeDetection | None = None
    level_gauge_detection_parameters: LevelGaugeDetection | None = None
    valve_detection_parameters: ValveDetection | None = None


class VisionJob(ContextualizationJob):
    def update_status(self) -> str:
        # Override update_status since we need to handle the vision-specific
        # edge case where we also record failed items per batch
        data = getattr(self._cognite_client, self._JOB_TYPE.value)._get(f"{self._status_path}{self.job_id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage") or data.get("failedItems")
        self._result = {k: v for k, v in data.items() if k not in self._COMMON_FIELDS}
        assert self.status is not None
        return self.status


class VisionExtractItem(CogniteResource):
    """Data class for storing predictions for a single image file"""

    def __init__(
        self,
        file_id: int | None = None,
        predictions: dict[str, Any] | None = None,
        file_external_id: str | None = None,
        error_message: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.error_message = error_message
        self.predictions = self._process_predictions_dict(predictions) if isinstance(predictions, dict) else predictions

        self._predictions_dict = predictions  # The "raw" predictions dict returned by the endpoint
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> VisionExtractItem:
        """Override CogniteResource._load so that we can convert the dicts returned by the API to data classes"""
        extracted_item = super()._load(resource, cognite_client=cognite_client)
        if isinstance(extracted_item.predictions, dict):
            extracted_item._predictions_dict = extracted_item.predictions
            extracted_item.predictions = cls._process_predictions_dict(extracted_item._predictions_dict)
        return extracted_item

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        item_dump = super().dump(camel_case=camel_case)
        # Replace the loaded VisionExtractPredictions with its corresponding dict representation
        if "predictions" in item_dump and isinstance(self._predictions_dict, dict):
            item_dump["predictions"] = (
                self._predictions_dict if camel_case else self._resource_to_snake_case(self._predictions_dict)
            )
        return item_dump

    @classmethod
    def _process_predictions_dict(cls, predictions_dict: dict[str, Any]) -> VisionExtractPredictions:
        """Converts a (validated) predictions dict to a corresponding VisionExtractPredictions"""
        prediction_object = VisionExtractPredictions()
        snake_case_predictions_dict = cls._resource_to_snake_case(predictions_dict)
        for key, value in snake_case_predictions_dict.items():
            if hasattr(prediction_object, key):
                feature_class = VISION_FEATURE_MAP[key]
                setattr(prediction_object, key, [feature_class(**v) for v in value])
        return prediction_object

    @classmethod
    def _resource_to_snake_case(cls, resource: Any) -> Any:
        if isinstance(resource, list):
            return [cls._resource_to_snake_case(element) for element in resource]
        elif isinstance(resource, dict):
            return {to_snake_case(k): cls._resource_to_snake_case(v) for k, v in resource.items() if v is not None}
        elif hasattr(resource, "__dict__"):
            return cls._resource_to_snake_case(resource.__dict__)
        return resource


class VisionExtractJob(VisionJob):
    _JOB_TYPE = ContextualizationJobType.VISION

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._items: list[VisionExtractItem] | None = None

    def __getitem__(self, file_id: int) -> VisionExtractItem:
        """Retrieves the results for a file by id"""
        found = [item for item in self.result["items"] if item.get("fileId") == file_id]
        if not found:
            raise IndexError(f"File with id {file_id} not found in results")
        return VisionExtractItem._load(found[0], cognite_client=self._cognite_client)

    @property
    def items(self) -> list[VisionExtractItem] | None:
        """Returns a list of all predictions by file"""
        if JobStatus(self.status) is JobStatus.COMPLETED:
            self._items = [
                VisionExtractItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items: list[VisionExtractItem]) -> None:
        self._items = items

    @property
    def errors(self) -> list[str]:
        """Returns a list of all error messages across files"""
        return [item["errorMessage"] for item in self.result["items"] if "errorMessage" in item]

    def _predictions_to_annotations(
        self,
        creating_user: str | None = None,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
    ) -> list[Annotation]:
        annotations = []

        for item in self.items or []:
            if item.predictions is not None:
                for prediction_type, prediction_data_list in item.predictions.dump().items():
                    for data in prediction_data_list:
                        # check if multiple annotations represent the same prediction (e.g. in case of dial gauges, level gauges and valves)
                        annotation_type = VISION_ANNOTATION_TYPE_MAP[prediction_type]
                        if isinstance(annotation_type, dict):
                            for key, value in annotation_type.items():
                                annotation = Annotation(
                                    annotated_resource_id=item.file_id,
                                    annotation_type=value,
                                    data=data[key],
                                    annotated_resource_type="file",
                                    status="suggested",
                                    creating_app=creating_app or "cognite-sdk-python",
                                    creating_app_version=creating_app_version or self._cognite_client.version,
                                    creating_user=creating_user or None,
                                )
                                annotations.append(annotation)
                        elif isinstance(annotation_type, str):
                            annotation = Annotation(
                                annotated_resource_id=item.file_id,
                                annotation_type=annotation_type,
                                data=data,
                                annotated_resource_type="file",
                                status="suggested",
                                creating_app=creating_app or "cognite-sdk-python",
                                creating_app_version=creating_app_version or self._cognite_client.version,
                                creating_user=creating_user or None,
                            )

                            annotations.append(annotation)
                        else:
                            raise TypeError("annotation_type must be str or dict")

        return annotations

    def save_predictions(
        self,
        creating_user: str | None = None,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
    ) -> Annotation | AnnotationList:
        """
        Saves all predictions made by the feature extractors in CDF using the Annotations API.
        See https://docs.cognite.com/api/v1/#operation/annotationsSuggest

        Args:
            creating_user (str | None): (str, optional): A username, or email, or name. This is not checked nor enforced. If the value is None, it means the annotation was created by a service.
            creating_app (str | None): The name of the app from which this annotation was created. Defaults to 'cognite-sdk-python'.
            creating_app_version (str | None): The version of the app that created this annotation. Must be a valid semantic versioning (SemVer) string. Defaults to client version.
        Returns:
            Annotation | AnnotationList: (suggested) annotation(s) stored in CDF.

        """
        if JobStatus(self.status) is JobStatus.COMPLETED:
            annotations = self._predictions_to_annotations(
                creating_user=creating_user, creating_app=creating_app, creating_app_version=creating_app_version
            )

            return self._cognite_client.annotations.suggest(annotations=annotations if annotations else [])

        raise CogniteException(
            "Extract job is not completed. If the job is queued or running, wait for completion and try again"
        )


class ResourceReference(CogniteResource):
    def __init__(
        self, id: int | None = None, external_id: str | None = None, cognite_client: None | CogniteClient = None
    ) -> None:
        self.id = id
        self.external_id = external_id
        self._cognite_client: CogniteClient = cast("CogniteClient", None)  # Read only


class ResourceReferenceList(CogniteResourceList[ResourceReference]):
    _RESOURCE = ResourceReference
