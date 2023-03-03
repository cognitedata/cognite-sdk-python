import time
import warnings
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, cast

from cognite.client.data_classes import Annotation
from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.annotation_types.images import AssetLink, ObjectDetection, TextRegion
from cognite.client.data_classes.annotation_types.primitives import VisionResource
from cognite.client.exceptions import CogniteAPIError, CogniteException, ModelFailedException
from cognite.client.utils._auxiliary import convert_true_match, exactly_one_is_not_none, to_snake_case


class JobStatus(Enum):
    _NOT_STARTED = None
    QUEUED = "Queued"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    DISTRIBUTING = "Distributing"
    DISTRIBUTED = "Distributed"
    COLLECTING = "Collecting"

    def is_finished(self):
        return self in {JobStatus.COMPLETED, JobStatus.FAILED}

    def is_not_finished(self):
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
    _COMMON_FIELDS = frozenset(
        {"status", "jobId", "modelId", "pipelineId", "errorMessage", "createdTime", "startTime", "statusTime"}
    )
    _JOB_TYPE = ContextualizationJobType.ENTITY_MATCHING

    def __init__(
        self,
        job_id=None,
        model_id=None,
        status=None,
        error_message=None,
        created_time=None,
        start_time=None,
        status_time=None,
        status_path=None,
        cognite_client=None,
    ):
        self.job_id = job_id
        self.model_id = model_id
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.status_time = status_time
        self.error_message = error_message
        self._cognite_client = cognite_client
        self._result = None
        self._status_path = status_path

    def update_status(self):
        data = getattr(self._cognite_client, self._JOB_TYPE.value)._get(f"{self._status_path}{self.job_id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        self._result = {k: v for (k, v) in data.items() if (k not in self._COMMON_FIELDS)}
        assert self.status is not None
        return self.status

    def wait_for_completion(self, timeout=None, interval=1):
        start = time.time()
        while (timeout is None) or (time.time() < (start + timeout)):
            self.update_status()
            if JobStatus(self.status).is_finished():
                break
            time.sleep(interval)
        if JobStatus(self.status) is JobStatus.FAILED:
            raise ModelFailedException(self.__class__.__name__, cast(int, self.job_id), cast(str, self.error_message))

    @property
    def result(self):
        if not self._result:
            self.wait_for_completion()
        assert self._result is not None
        return self._result

    def __str__(self):
        return f"{self.__class__.__name__}(id={self.job_id}, status={self.status}, error={self.error_message})"

    @classmethod
    def _load_with_status(cls, data, status_path, cognite_client):
        obj = cls._load(data, cognite_client=cognite_client)
        obj._status_path = status_path
        return obj


T_ContextualizationJob = TypeVar("T_ContextualizationJob", bound=ContextualizationJob)


class ContextualizationJobList(CogniteResourceList):
    _RESOURCE = ContextualizationJob


class EntityMatchingModel(CogniteResource):
    _RESOURCE_PATH = "/context/entitymatching"
    _STATUS_PATH = _RESOURCE_PATH + "/"

    def __init__(
        self,
        id=None,
        status=None,
        error_message=None,
        created_time=None,
        start_time=None,
        status_time=None,
        classifier=None,
        feature_type=None,
        match_fields=None,
        model_type=None,
        name=None,
        description=None,
        external_id=None,
        cognite_client=None,
    ):
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
        self._cognite_client = cognite_client

    def __str__(self):
        return f"{self.__class__.__name__}(id={self.id}, status={self.status}, error={self.error_message})"

    def update_status(self):
        data = self._cognite_client.entity_matching._get(f"{self._STATUS_PATH}{self.id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        assert self.status is not None
        return self.status

    def wait_for_completion(self, timeout=None, interval=1):
        start = time.time()
        while (timeout is None) or (time.time() < (start + timeout)):
            self.update_status()
            if JobStatus(self.status) not in [JobStatus.QUEUED, JobStatus.RUNNING]:
                break
            time.sleep(interval)
        if JobStatus(self.status) is JobStatus.FAILED:
            assert self.id is not None
            assert self.error_message is not None
            raise ModelFailedException(self.__class__.__name__, self.id, self.error_message)

    def predict(self, sources=None, targets=None, num_matches=1, score_threshold=None):
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

    def refit(self, true_matches):
        true_matches = [convert_true_match(true_match) for true_match in true_matches]
        self.wait_for_completion()
        response = self._cognite_client.entity_matching._post(
            (self._RESOURCE_PATH + "/refit"), json={"trueMatches": true_matches, "id": self.id}
        )
        return self._load(response.json(), cognite_client=self._cognite_client)

    @staticmethod
    def _flatten_entity(entity):
        if isinstance(entity, CogniteResource):
            entity = entity.dump(camel_case=True)
        if ("metadata" in entity) and isinstance(entity["metadata"], dict):
            for (k, v) in entity["metadata"].items():
                entity[f"metadata.{k}"] = v
        return {k: v for (k, v) in entity.items() if ((k == "id") or isinstance(v, str))}

    @staticmethod
    def _dump_entities(entities):
        if entities:
            return [EntityMatchingModel._flatten_entity(e) for e in entities]
        return None


class EntityMatchingModelUpdate(CogniteUpdate):
    class _PrimitiveUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    @property
    def name(self):
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "name")

    @property
    def description(self):
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "description")


class EntityMatchingModelList(CogniteResourceList):
    _RESOURCE = EntityMatchingModel


class FileReference:
    def __init__(self, file_id=None, file_external_id=None, first_page=None, last_page=None):
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.first_page = first_page
        self.last_page = last_page
        if not exactly_one_is_not_none(file_id, file_external_id):
            raise ValueError("Exactly one of file_id and file_external_id must be set for a file reference")
        if exactly_one_is_not_none(first_page, last_page):
            raise ValueError("If the page range feature is used, both first page and last page must be set")

    def to_api_item(self):
        if (self.file_id is None) and (self.file_external_id is not None):
            item: Dict[(str, Union[(str, int, Dict[(str, int)])])] = {"fileExternalId": self.file_external_id}
        if (self.file_id is not None) and (self.file_external_id is None):
            item = {"fileId": self.file_id}
        if (self.first_page is not None) and (self.last_page is not None):
            item["pageRange"] = {"begin": self.first_page, "end": self.last_page}
        return item


class DiagramConvertPage(CogniteResource):
    def __init__(self, page=None, png_url=None, svg_url=None, cognite_client=None):
        self.page = page
        self.png_url = png_url
        self.svg_url = svg_url
        self._cognite_client = cognite_client


class DiagramConvertPageList(CogniteResourceList):
    _RESOURCE = DiagramConvertPage


class DiagramConvertItem(CogniteResource):
    def __init__(self, file_id=None, file_external_id=None, results=None, cognite_client=None):
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.results = results
        self._cognite_client = cognite_client

    def __len__(self):
        assert self.results
        return len(self.results)

    @property
    def pages(self):
        assert self.results is not None
        return DiagramConvertPageList._load(self.results, cognite_client=self._cognite_client)

    def to_pandas(self, camel_case=False):
        df = super().to_pandas(camel_case=camel_case)
        df.loc["results"] = f"{len(df['results'])} pages"
        return df


class DiagramConvertResults(ContextualizationJob):
    _JOB_TYPE = ContextualizationJobType.DIAGRAMS

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._items: Optional[list] = None

    def __getitem__(self, find_id):
        found = [
            item
            for item in self.result["items"]
            if ((item.get("fileId") == find_id) or (item.get("fileExternalId") == find_id))
        ]
        if not found:
            raise IndexError(f"File with (external) id {find_id} not found in results")
        if len(found) != 1:
            raise IndexError(f"Found multiple results for file with (external) id {find_id}, use .items instead")
        return DiagramConvertItem._load(found[0], cognite_client=self._cognite_client)

    @property
    def items(self):
        if JobStatus(self.status) is JobStatus.COMPLETED:
            self._items = [
                DiagramConvertItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items):
        self._items = items


class DiagramDetectItem(CogniteResource):
    def __init__(
        self,
        file_id=None,
        file_external_id=None,
        annotations=None,
        error_message=None,
        cognite_client=None,
        page_range=None,
    ):
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.annotations = annotations
        self.error_message = error_message
        self._cognite_client = cognite_client
        self.page_range = page_range

    def to_pandas(self, camel_case=False):
        df = super().to_pandas(camel_case=camel_case)
        df.loc["annotations"] = f"{len(df['annotations'])} annotations"
        return df


class DiagramDetectResults(ContextualizationJob):
    _JOB_TYPE = ContextualizationJobType.DIAGRAMS

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._items: Optional[List[DiagramDetectItem]] = None

    def __getitem__(self, find_id):
        found = [
            item
            for item in self.result["items"]
            if ((item.get("fileId") == find_id) or (item.get("fileExternalId") == find_id))
        ]
        if not found:
            raise IndexError(f"File with (external) id {find_id} not found in results")
        if len(found) != 1:
            raise IndexError(f"Found multiple results for file with (external) id {find_id}, use .items instead")
        return DiagramDetectItem._load(found[0], cognite_client=self._cognite_client)

    @property
    def items(self):
        if JobStatus(self.status) is JobStatus.COMPLETED:
            self._items = [
                DiagramDetectItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items):
        self._items = items

    @property
    def errors(self):
        return [item["errorMessage"] for item in self.result["items"] if ("errorMessage" in item)]

    def convert(self):
        return self._cognite_client.diagrams.convert(detect_job=self)


FeatureClass = Union[(Type[TextRegion], Type[AssetLink], Type[ObjectDetection])]


class VisionFeature(str, Enum):
    TEXT_DETECTION = "TextDetection"
    ASSET_TAG_DETECTION = "AssetTagDetection"
    PEOPLE_DETECTION = "PeopleDetection"
    INDUSTRIAL_OBJECT_DETECTION = "IndustrialObjectDetection"
    PERSONAL_PROTECTIVE_EQUIPMENT_DETECTION = "PersonalProtectiveEquipmentDetection"

    @classmethod
    def beta_features(cls):
        return {cls.INDUSTRIAL_OBJECT_DETECTION, cls.PERSONAL_PROTECTIVE_EQUIPMENT_DETECTION}


class VisionExtractPredictions(VisionResource):
    text_predictions: Optional[List[TextRegion]] = None
    asset_tag_predictions: Optional[List[AssetLink]] = None
    industrial_object_predictions: Optional[List[ObjectDetection]] = None
    people_predictions: Optional[List[ObjectDetection]] = None
    personal_protective_equipment_predictions: Optional[List[ObjectDetection]] = None

    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


VISION_FEATURE_MAP = {
    "text_predictions": TextRegion,
    "asset_tag_predictions": AssetLink,
    "industrial_object_predictions": ObjectDetection,
    "people_predictions": ObjectDetection,
    "personal_protective_equipment_predictions": ObjectDetection,
}
VISION_ANNOTATION_TYPE_MAP: Dict[(str, str)] = {
    "text_predictions": "images.TextRegion",
    "asset_tag_predictions": "images.AssetLink",
    "industrial_object_predictions": "images.ObjectDetection",
    "people_predictions": "images.ObjectDetection",
    "personal_protective_equipment_predictions": "images.ObjectDetection",
}


class ThresholdParameter:
    threshold: Optional[float] = None

    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class AssetTagDetectionParameters(VisionResource, ThresholdParameter):
    partial_match: Optional[bool] = None
    asset_subtree_ids: Optional[List[int]] = None

    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class TextDetectionParameters(VisionResource, ThresholdParameter):
    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class PeopleDetectionParameters(VisionResource, ThresholdParameter):
    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class IndustrialObjectDetectionParameters(VisionResource, ThresholdParameter):
    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class PersonalProtectiveEquipmentDetectionParameters(VisionResource, ThresholdParameter):
    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class DetectJobBundle:
    _RESOURCE_PATH = "/context/diagram/detect/"
    _STATUS_PATH = "/context/diagram/detect/status"
    _WAIT_TIME = 2

    def __init__(self, job_ids, cognite_client=None):
        warnings.warn(
            "DetectJobBundle.result is calling a beta endpoint which is still in development. Breaking changes can happen in between patch versions."
        )
        self._cognite_client = cognite_client
        if not job_ids:
            raise ValueError("You need to specify job_ids")
        self.job_ids = job_ids
        self._remaining_job_ids: List[int] = []
        self.jobs: List[Dict[(str, Any)]] = []
        self._result: List[Dict[(str, Any)]] = []

    def __str__(self):
        return f"DetectJobBundle(self.job_ids={self.job_ids}, self.jobs={self.jobs}, self._result={self._result}, self._remaining_job_ids={self._remaining_job_ids})"

    def _back_off(self):
        time.sleep(self._WAIT_TIME)
        if self._WAIT_TIME < 10:
            self._WAIT_TIME += 2

    def wait_for_completion(self, timeout=None):
        start = time.time()
        self._remaining_job_ids = self.job_ids
        while (timeout is None) or (time.time() < (start + timeout)):
            try:
                res = self._cognite_client.diagrams._post(self._STATUS_PATH, json={"items": self._remaining_job_ids})
            except CogniteAPIError:
                self._back_off()
                continue
            if res.json().get("error"):
                break
            self.jobs = res.json()["items"]
            self._remaining_job_ids = [j["jobId"] for j in self.jobs if JobStatus(j["status"]).is_not_finished()]
            if self._remaining_job_ids:
                self._back_off()
            else:
                self._WAIT_TIME = 2
                break

    def fetch_results(self):
        return [self._cognite_client.diagrams._get(f"{self._RESOURCE_PATH}{j}").json() for j in self.job_ids]

    @property
    def result(self):
        if not self._result:
            self.wait_for_completion()
            self._result = self.fetch_results()
        assert self._result is not None
        failed: List[Dict[(str, Any)]] = []
        succeeded: List[Dict[(str, Any)]] = []
        for job_result in self._result:
            for item in job_result["items"]:
                if "errorMessage" in item:
                    failed.append({**item, **{"job_id": job_result["jobId"], "status": job_result["status"]}})
                else:
                    succeeded.append({**item, **{"job_id": job_result["jobId"], "status": job_result["status"]}})
        return (succeeded, failed)


class FeatureParameters(VisionResource):
    text_detection_parameters: Optional[TextDetectionParameters] = None
    asset_tag_detection_parameters: Optional[AssetTagDetectionParameters] = None
    people_detection_parameters: Optional[PeopleDetectionParameters] = None
    industrial_object_detection_parameters: Optional[IndustrialObjectDetectionParameters] = None
    personal_protective_equipment_detection_parameters: Optional[PersonalProtectiveEquipmentDetectionParameters] = None

    def __init__(self, *a, **kw):
        raise NotImplementedError("Not support in Python 3.6")


class VisionJob(ContextualizationJob):
    def update_status(self):
        data = getattr(self._cognite_client, self._JOB_TYPE.value)._get(f"{self._status_path}{self.job_id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage") or data.get("failedItems")
        self._result = {k: v for (k, v) in data.items() if (k not in self._COMMON_FIELDS)}
        assert self.status is not None
        return self.status


class VisionExtractItem(CogniteResource):
    def __init__(self, file_id=None, predictions=None, file_external_id=None, error_message=None, cognite_client=None):
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.error_message = error_message
        self.predictions = self._process_predictions_dict(predictions) if isinstance(predictions, Dict) else predictions
        self._predictions_dict = predictions
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource, cognite_client=None):
        extracted_item = super()._load(resource, cognite_client=cognite_client)
        if isinstance(extracted_item.predictions, dict):
            extracted_item._predictions_dict = extracted_item.predictions
            extracted_item.predictions = cls._process_predictions_dict(extracted_item._predictions_dict)
        return extracted_item

    def dump(self, camel_case=False):
        item_dump = super().dump(camel_case=camel_case)
        if ("predictions" in item_dump) and isinstance(self._predictions_dict, Dict):
            item_dump["predictions"] = (
                self._predictions_dict if camel_case else self._resource_to_snake_case(self._predictions_dict)
            )
        return item_dump

    @classmethod
    def _process_predictions_dict(cls, predictions_dict):
        prediction_object = VisionExtractPredictions()
        snake_case_predictions_dict = cls._resource_to_snake_case(predictions_dict)
        for (key, value) in snake_case_predictions_dict.items():
            if hasattr(prediction_object, key):
                feature_class = VISION_FEATURE_MAP[key]
                setattr(prediction_object, key, [feature_class(**v) for v in value])
        return prediction_object

    @classmethod
    def _resource_to_snake_case(cls, resource):
        if isinstance(resource, list):
            return [cls._resource_to_snake_case(element) for element in resource]
        elif isinstance(resource, dict):
            return {to_snake_case(k): cls._resource_to_snake_case(v) for (k, v) in resource.items() if (v is not None)}
        elif hasattr(resource, "__dict__"):
            return cls._resource_to_snake_case(resource.__dict__)
        return resource


class VisionExtractJob(VisionJob):
    _JOB_TYPE = ContextualizationJobType.VISION

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._items: Optional[List[VisionExtractItem]] = None

    def __getitem__(self, file_id):
        found = [item for item in self.result["items"] if (item.get("fileId") == file_id)]
        if not found:
            raise IndexError(f"File with id {file_id} not found in results")
        return VisionExtractItem._load(found[0], cognite_client=self._cognite_client)

    @property
    def items(self):
        if JobStatus(self.status) is JobStatus.COMPLETED:
            self._items = [
                VisionExtractItem._load(item, cognite_client=self._cognite_client) for item in self.result["items"]
            ]
        return self._items

    @items.setter
    def items(self, items):
        self._items = items

    @property
    def errors(self):
        return [item["errorMessage"] for item in self.result["items"] if ("errorMessage" in item)]

    def _predictions_to_annotations(self, creating_user=None, creating_app=None, creating_app_version=None):
        return [
            Annotation(
                annotated_resource_id=item.file_id,
                annotation_type=VISION_ANNOTATION_TYPE_MAP[prediction_type],
                data=data.dump(),
                annotated_resource_type="file",
                status="suggested",
                creating_app=(creating_app or "cognite-sdk-python"),
                creating_app_version=(creating_app_version or self._cognite_client.version),
                creating_user=(creating_user or None),
            )
            for item in (self.items or [])
            if (item.predictions is not None)
            for (prediction_type, prediction_data_list) in item.predictions.dump().items()
            for data in prediction_data_list
        ]

    def save_predictions(self, creating_user=None, creating_app=None, creating_app_version=None):
        if JobStatus(self.status) is JobStatus.COMPLETED:
            annotations = self._predictions_to_annotations(
                creating_user=creating_user, creating_app=creating_app, creating_app_version=creating_app_version
            )
            if annotations:
                return self._cognite_client.annotations.suggest(annotations=annotations)
            raise CogniteException("Extract job does not contain any predictions.")
        raise CogniteException(
            "Extract job is not completed. If the job is queued or running, wait for completion and try again"
        )
