from __future__ import annotations

import asyncio
import random
import time
import warnings
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, cast, final

from typing_extensions import Self

from cognite.client.data_classes import Annotation, AnnotationWrite
from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    IdTransformerMixin,
    PropertySpec,
)
from cognite.client.data_classes.annotation_types.images import (
    AssetLink,
    KeypointCollectionWithObjectDetection,
    ObjectDetection,
    TextRegion,
)
from cognite.client.data_classes.annotation_types.primitives import VisionResource
from cognite.client.data_classes.annotations import AnnotationList, AnnotationType
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.exceptions import CogniteAPIError, ModelFailedException
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils._auxiliary import convert_true_match, exactly_one_is_not_none, load_resource
from cognite.client.utils._text import convert_all_keys_to_snake_case, copy_doc_from_async, to_camel_case

if TYPE_CHECKING:
    import pandas

    from cognite.client import AsyncCogniteClient


class JobStatus(Enum):
    _NOT_STARTED = None
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


class ContextualizationJob(CogniteResource, ABC):
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

    def __init__(
        self,
        job_id: int,
        status: str,
        status_time: int,
        created_time: int,
        error_message: str | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.job_id = job_id
        self.status = status
        self.created_time = created_time
        self.status_time = status_time
        self.error_message = error_message
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)
        self._result: dict[str, Any] | None = None
        self.job_token: str | None = None

    @abstractmethod
    async def update_status_async(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def update_status(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _status_path(self) -> str:
        raise NotImplementedError

    async def wait_for_completion(self, timeout: float | None = None, interval: float = 10) -> None:
        """Waits for job completion. This is generally not needed to call directly, as `.result` will do so automatically.

        Args:
            timeout (float | None): Time out after this many seconds. (None means wait indefinitely)
            interval (float): Influence how often to poll status (seconds).

        Raises:
            ModelFailedException: The model fit failed.
        """
        start = time.time()
        while timeout is None or time.time() < start + timeout:
            await self.update_status_async()
            if JobStatus(self.status).is_finished():
                break
            await asyncio.sleep(max(1, random.uniform(0, interval)))

        if JobStatus(self.status) is JobStatus.FAILED:
            raise ModelFailedException(type(self).__name__, self.job_id, cast(str, self.error_message))

    async def wait_for_result(self) -> dict[str, Any]:
        """Waits for the job to finish and returns the results."""
        if not self._result:
            await self.wait_for_completion()
        assert self._result is not None
        return self._result

    def __str__(self) -> str:
        return f"{type(self).__name__}(id={self.job_id}, status={self.status}, error={self.error_message})"

    @classmethod
    def _load_with_job_token(
        cls: type[T_ContextualizationJob],
        data: dict[str, Any],
        *,
        headers: MutableMapping[str, str],
        cognite_client: Any,
    ) -> T_ContextualizationJob:
        obj = cls._load(data, cognite_client=cognite_client)
        obj.job_token = headers.get("X-Job-Token")
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
        id: int,
        created_time: int,
        status: str | None = None,
        error_message: str | None = None,
        start_time: int | None = None,
        status_time: int | None = None,
        classifier: str | None = None,
        feature_type: str | None = None,
        match_fields: list[str] | None = None,
        model_type: str | None = None,
        name: str | None = None,
        description: str | None = None,
        external_id: str | None = None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.id = id
        self.created_time = created_time
        self.status = status
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
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            status=resource.get("status"),
            error_message=resource.get("errorMessage"),
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            status_time=resource.get("statusTime"),
            classifier=resource.get("classifier"),
            feature_type=resource.get("featureType"),
            match_fields=resource.get("matchFields"),
            model_type=resource.get("modelType"),
            name=resource.get("name"),
            description=resource.get("description"),
            external_id=resource.get("externalId"),
            cognite_client=cognite_client,
        )

    def __str__(self) -> str:
        return f"{type(self).__name__}(id={self.id}, status={self.status}, error={self.error_message})"

    async def update_status_async(self) -> str:
        """Updates the model status and returns it"""
        data = (await self._cognite_client.entity_matching._get(f"{self._STATUS_PATH}{self.id}")).json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        assert self.status is not None
        return self.status

    @copy_doc_from_async(update_status_async)
    def update_status(self) -> str:
        return run_sync(self.update_status_async())

    async def wait_for_completion_async(self, timeout: int | None = None, interval: int = 10) -> None:
        """Waits for model completion. This is generally not needed to call directly, as `.result` will do so automatically.

        Args:
            timeout (int | None): Time out after this many seconds. (None means wait indefinitely)
            interval (int): Influence how often to poll status (seconds).

        Raises:
            ModelFailedException: The model fit failed.
        """
        start = time.time()
        while timeout is None or time.time() < start + timeout:
            await self.update_status_async()
            if JobStatus(self.status) not in [JobStatus.QUEUED, JobStatus.RUNNING]:
                break
            await asyncio.sleep(max(1, random.uniform(0, interval)))

        if JobStatus(self.status) is JobStatus.FAILED:
            assert self.id is not None
            assert self.error_message is not None
            raise ModelFailedException(type(self).__name__, self.id, self.error_message)

    @copy_doc_from_async(wait_for_completion_async)
    def wait_for_completion(self, timeout: int | None = None, interval: int = 10) -> None:
        return run_sync(self.wait_for_completion_async(timeout=timeout, interval=interval))

    async def predict_async(
        self,
        sources: list[dict] | None = None,
        targets: list[dict] | None = None,
        num_matches: int = 1,
        score_threshold: float | None = None,
    ) -> EntityMatchingPredictionResult:
        """Predict entity matching.

        Note:
            Blocks and waits for the model to be ready if it has been recently created.

        Args:
            sources (list[dict] | None): entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets (list[dict] | None): entities to match to, does not need an 'id' field. Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float | None): only return matches with a score above this threshold

        Returns:
            EntityMatchingPredictionResult: object which can be used to wait for and retrieve results."""
        await self.wait_for_completion_async()
        json = {
            "id": self.id,
            "sources": self._dump_entities(sources),
            "targets": self._dump_entities(targets),
            "numMatches": num_matches,
            "scoreThreshold": score_threshold,
        }
        response = await self._cognite_client.entity_matching._post(f"{self._RESOURCE_PATH}/predict", json=json)
        return EntityMatchingPredictionResult._load_with_job_token(
            data=response.json(),
            headers=response.headers,
            cognite_client=self._cognite_client,
        )

    @copy_doc_from_async(predict_async)
    def predict(
        self,
        sources: list[dict] | None = None,
        targets: list[dict] | None = None,
        num_matches: int = 1,
        score_threshold: float | None = None,
    ) -> EntityMatchingPredictionResult:
        return run_sync(
            self.predict_async(
                sources=sources, targets=targets, num_matches=num_matches, score_threshold=score_threshold
            )
        )

    async def refit_async(self, true_matches: Sequence[dict | tuple[int | str, int | str]]) -> EntityMatchingModel:
        """Re-fits an entity matching model, using the combination of the old and new true matches.

        Args:
            true_matches (Sequence[dict | tuple[int | str, int | str]]): Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId'). A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
        Returns:
            EntityMatchingModel: new model refitted to true_matches."""
        true_matches = [convert_true_match(true_match) for true_match in true_matches]
        await self.wait_for_completion_async()
        response = await self._cognite_client.entity_matching._post(
            self._RESOURCE_PATH + "/refit", json={"trueMatches": true_matches, "id": self.id}
        )
        return self._load(response.json(), cognite_client=self._cognite_client)

    @copy_doc_from_async(refit_async)
    def refit(self, true_matches: Sequence[dict | tuple[int | str, int | str]]) -> EntityMatchingModel:
        return run_sync(self.refit_async(true_matches=true_matches))

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
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
        ]


class EntityMatchingModelList(CogniteResourceList[EntityMatchingModel], IdTransformerMixin):
    _RESOURCE = EntityMatchingModel


class FileReference:
    def __init__(
        self,
        file_id: int | None = None,
        file_external_id: str | None = None,
        file_instance_id: NodeId | None = None,
        first_page: int | None = None,
        last_page: int | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.file_instance_id = file_instance_id
        self.first_page = first_page
        self.last_page = last_page

        if not exactly_one_is_not_none(file_id, file_external_id, file_instance_id):
            raise ValueError("File references must have exactly one of file_id, file_external_id and file_instance_id.")
        if exactly_one_is_not_none(first_page, last_page):
            raise ValueError("If the page range feature is used, both first page and last page must be set")

    def to_api_item(self) -> dict[str, str | int | dict[str, int] | dict[str, str]]:
        if self.file_id is None and self.file_external_id is not None and self.file_instance_id is None:
            item: dict[str, str | int | dict[str, int] | dict[str, str]] = {"fileExternalId": self.file_external_id}
        if self.file_id is not None and self.file_external_id is None and self.file_instance_id is None:
            item = {"fileId": self.file_id}
        if self.file_id is None and self.file_external_id is None and self.file_instance_id is not None:
            item = {"fileInstanceId": self.file_instance_id.dump(include_instance_type=False)}

        if self.first_page is not None and self.last_page is not None:
            item["pageRange"] = {"begin": self.first_page, "end": self.last_page}
        return item


class DiagramConvertPage(CogniteResource):
    def __init__(
        self,
        page: int,
        png_url: str,
        svg_url: str,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.page = page
        self.png_url = png_url
        self.svg_url = svg_url
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            page=resource["page"],
            png_url=resource["pngUrl"],
            svg_url=resource["svgUrl"],
            cognite_client=cognite_client,
        )


class DiagramConvertPageList(CogniteResourceList[DiagramConvertPage]):
    _RESOURCE = DiagramConvertPage


class DiagramConvertItem(CogniteResource):
    def __init__(
        self,
        file_id: int,
        file_external_id: str | None,
        results: list[dict[str, Any]],
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.results = results
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            file_id=resource["fileId"],
            file_external_id=resource.get("fileExternalId"),
            results=resource["results"],
            cognite_client=cognite_client,
        )

    def __len__(self) -> int:
        assert self.results
        return len(self.results)

    @property
    def pages(self) -> DiagramConvertPageList:
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


@final
class DiagramConvertResults(ContextualizationJob):
    def __init__(
        self,
        job_id: int,
        status: str,
        status_time: int,
        created_time: int,
        items: list[DiagramConvertItem],
        start_time: int,
        error_message: str | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        super().__init__(
            job_id=job_id,
            status=status,
            status_time=status_time,
            created_time=created_time,
            error_message=error_message,
            cognite_client=cognite_client,
        )
        self.items = items
        self.start_time = start_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource["startTime"],
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            items=[DiagramConvertItem._load(item, cognite_client=cognite_client) for item in resource["items"]],
            cognite_client=cognite_client,
        )

    def _status_path(self) -> str:
        return f"/context/diagram/convert/{self.job_id}"

    async def update_status_async(self) -> str:
        """Updates the model status and returns it"""
        job_token = self.job_token
        headers = {"X-Job-Token": job_token} if job_token else {}
        resource = (await self._cognite_client.diagrams._get(self._status_path(), headers=headers)).json()
        self.__init__(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource["startTime"],
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            items=[DiagramConvertItem._load(item, cognite_client=self._cognite_client) for item in resource["items"]],
            cognite_client=self._cognite_client,
        )
        self.job_token = job_token
        self._result = {k: v for k, v in resource.items() if k not in self._COMMON_FIELDS}
        return self.status

    @copy_doc_from_async(update_status_async)
    def update_status(self) -> str:
        return run_sync(self.update_status_async())

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["items"] = [item.dump(camel_case=camel_case) for item in self.items]
        return data

    def __getitem__(self, find_id: int | str) -> DiagramConvertItem:
        """Retrieves the results for the file with (external) id"""
        found = [item for item in self.items if item.file_id == find_id or item.file_external_id == find_id]
        if not found:
            raise IndexError(f"File with (external) id {find_id} not found in results")
        if len(found) != 1:
            raise IndexError(f"Found multiple results for file with (external) id {find_id}, use .items instead")
        return found[0]


class DiagramDetectItem(CogniteResource):
    def __init__(
        self,
        file_id: int,
        file_external_id: str | None,
        file_instance_id: dict[str, str] | None,
        annotations: list[dict[str, Any]] | None,
        page_range: dict[str, int] | None,
        page_count: int | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.file_instance_id = file_instance_id
        self.annotations = annotations or []
        self.page_range = page_range
        self.page_count = page_count
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            file_id=resource["fileId"],
            file_external_id=resource.get("fileExternalId"),
            file_instance_id=resource.get("fileInstanceId"),
            annotations=resource.get("annotations"),
            cognite_client=cognite_client,
            page_range=resource.get("pageRange"),
            page_count=resource.get("pageCount"),
        )

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        df = super().to_pandas(camel_case=camel_case)
        df.loc["annotations"] = f"{len(self.annotations or [])} annotations"
        return df


@final
class DiagramDetectResults(ContextualizationJob):
    def __init__(
        self,
        job_id: int,
        status: str,
        status_time: int,
        created_time: int,
        items: list[DiagramDetectItem],
        start_time: int,
        error_message: str | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        super().__init__(
            job_id=job_id,
            status=status,
            status_time=status_time,
            created_time=created_time,
            error_message=error_message,
            cognite_client=cognite_client,
        )
        self.items: list[DiagramDetectItem] = items
        self.start_time = start_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource["startTime"],
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            items=[DiagramDetectItem._load(item, cognite_client=cognite_client) for item in resource["items"]],
            cognite_client=cognite_client,
        )

    def _status_path(self) -> str:
        return f"/context/diagram/detect/{self.job_id}"

    async def update_status_async(self) -> str:
        job_token = self.job_token
        headers = {"X-Job-Token": job_token} if job_token else {}
        resource = (await self._cognite_client.diagrams._get(self._status_path(), headers=headers)).json()
        self.__init__(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource["startTime"],
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            items=[
                DiagramDetectItem._load(item, cognite_client=self._cognite_client) for item in resource.get("items", [])
            ],
            cognite_client=self._cognite_client,
        )
        self.job_token = job_token
        self._result = {k: v for k, v in resource.items() if k not in self._COMMON_FIELDS}
        return self.status

    @copy_doc_from_async(update_status_async)
    def update_status(self) -> str:
        return run_sync(self.update_status_async())

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["items"] = [item.dump(camel_case=camel_case) for item in self.items]
        return data

    def __getitem__(self, find_id: Any) -> DiagramDetectItem:
        """retrieves the results for the file with (external) id"""
        found = [item for item in self.items if item.file_id == find_id or item.file_external_id == find_id]
        if not found:
            raise IndexError(f"File with (external) id {find_id} not found in results")
        if len(found) != 1:
            raise IndexError(f"Found multiple results for file with (external) id {find_id}, use .items instead")
        return found[0]

    async def errors_async(self) -> list[str]:
        """Returns a list of all error messages across files"""
        results = (await self.wait_for_result())["items"]
        return [item["errorMessage"] async for item in results if "errorMessage" in item]

    @copy_doc_from_async(errors_async)
    def errors(self) -> list[str]:
        return run_sync(self.errors_async())

    async def convert_async(self) -> DiagramConvertResults:
        """Convert a P&ID to an interactive SVG where the provided annotations are highlighted"""
        return await self._cognite_client.diagrams.convert(detect_job=self)

    @copy_doc_from_async(convert_async)
    def convert(self) -> DiagramConvertResults:
        return run_sync(self.convert_async())


# Vision dataclasses
FeatureClass = type[TextRegion] | type[AssetLink] | type[ObjectDetection]


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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: Any = None) -> VisionExtractPredictions:
        return cls(
            text_predictions=[
                TextRegion._load(text_prediction) for text_prediction in resource.get("textPredictions", [])
            ]
            or None,
            asset_tag_predictions=[
                AssetLink._load(asset_tag_prediction)
                for asset_tag_prediction in resource.get("assetTagPredictions", [])
            ]
            or None,
            industrial_object_predictions=[
                ObjectDetection._load(industrial_object_prediction)
                for industrial_object_prediction in resource.get("industrialObjectPredictions", [])
            ]
            or None,
            people_predictions=[
                ObjectDetection._load(people_prediction) for people_prediction in resource.get("peoplePredictions", [])
            ]
            or None,
            personal_protective_equipment_predictions=[
                ObjectDetection._load(personal_protective_equipment_prediction)
                for personal_protective_equipment_prediction in resource.get(
                    "personalProtectiveEquipmentPredictions", []
                )
            ]
            or None,
            digital_gauge_predictions=[
                ObjectDetection._load(digital_gauge_prediction)
                for digital_gauge_prediction in resource.get("digitalGaugePredictions", [])
            ]
            or None,
            dial_gauge_predictions=[
                KeypointCollectionWithObjectDetection._load(dial_gauge_prediction)
                for dial_gauge_prediction in resource.get("dialGaugePredictions", [])
            ]
            or None,
            level_gauge_predictions=[
                KeypointCollectionWithObjectDetection._load(level_gauge_prediction)
                for level_gauge_prediction in resource.get("levelGaugePredictions", [])
            ]
            or None,
            valve_predictions=[
                KeypointCollectionWithObjectDetection._load(valve_prediction)
                for valve_prediction in resource.get("valvePredictions", [])
            ]
            or None,
        )


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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            threshold=resource.get("threshold"),
            partial_match=resource.get("partialMatch"),
            asset_subtree_ids=resource.get("assetSubtreeIds"),
        )


@dataclass
class TextDetectionParameters(VisionResource, ThresholdParameter):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(threshold=resource.get("threshold"))


@dataclass
class PeopleDetectionParameters(VisionResource, ThresholdParameter):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(threshold=resource.get("threshold"))


@dataclass
class IndustrialObjectDetectionParameters(VisionResource, ThresholdParameter):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(threshold=resource.get("threshold"))


@dataclass
class PersonalProtectiveEquipmentDetectionParameters(VisionResource, ThresholdParameter):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(threshold=resource.get("threshold"))


@dataclass
class DialGaugeDetection(VisionResource, ThresholdParameter):
    min_level: float | None = None
    max_level: float | None = None
    dead_angle: float | None = None
    non_linear_angle: float | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            threshold=resource.get("threshold"),
            min_level=resource.get("minLevel"),
            max_level=resource.get("maxLevel"),
            dead_angle=resource.get("deadAngle"),
            non_linear_angle=resource.get("nonLinearAngle"),
        )


@dataclass
class LevelGaugeDetection(VisionResource, ThresholdParameter):
    min_level: float | None = None
    max_level: float | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            threshold=resource.get("threshold"),
            min_level=resource.get("minLevel"),
            max_level=resource.get("maxLevel"),
        )


@dataclass
class DigitalGaugeDetection(VisionResource, ThresholdParameter):
    comma_position: int | None = None
    min_num_digits: int | None = None
    max_num_digits: int | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            threshold=resource.get("threshold"),
            comma_position=resource.get("commaPosition"),
            min_num_digits=resource.get("minNumDigits"),
            max_num_digits=resource.get("maxNumDigits"),
        )


@dataclass
class ValveDetection(VisionResource, ThresholdParameter):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(threshold=resource.get("threshold"))


class DetectJobBundle:
    _RESOURCE_PATH = "/context/diagram/detect/"
    _STATUS_PATH = "/context/diagram/detect/status"
    _WAIT_TIME = 2

    def __init__(self, job_ids: list[int], cognite_client: AsyncCogniteClient | None = None) -> None:
        warnings.warn(
            "DetectJobBundle.result is calling a beta endpoint which is still in development. "
            "Breaking changes can happen in between patch versions."
        )

        self._cognite_client = cast("AsyncCogniteClient", cognite_client)
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

    async def wait_for_completion(self, timeout: int | None = None) -> None:
        """Waits for all jobs to complete, generally not needed to call as it is called by result.

        Args:
            timeout (int | None): Time out after this many seconds. (None means wait indefinitely)
        """
        start = time.time()
        self._remaining_job_ids = self.job_ids
        while timeout is None or time.time() < start + timeout:
            try:
                res = await self._cognite_client.diagrams._post(
                    self._STATUS_PATH, json={"items": self._remaining_job_ids}
                )
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

    async def fetch_results(self) -> list[dict[str, Any]]:
        return [(await self._cognite_client.diagrams._get(f"{self._RESOURCE_PATH}{j}")).json() for j in self.job_ids]

    async def wait_for_result(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Waits for the job to finish and returns the results."""
        if not self._result:
            await self.wait_for_completion()

            self._result = await self.fetch_results()
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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: Any = None) -> FeatureParameters:
        return cls(
            text_detection_parameters=load_resource(resource, TextDetectionParameters, "textDetectionParameters"),
            asset_tag_detection_parameters=load_resource(
                resource, AssetTagDetectionParameters, "assetTagDetectionParameters"
            ),
            people_detection_parameters=load_resource(resource, PeopleDetectionParameters, "peopleDetectionParameters"),
            industrial_object_detection_parameters=load_resource(
                resource, IndustrialObjectDetectionParameters, "industrialObjectDetectionParameters"
            ),
            personal_protective_equipment_detection_parameters=load_resource(
                resource,
                PersonalProtectiveEquipmentDetectionParameters,
                "personalProtectiveEquipmentDetectionParameters",
            ),
            digital_gauge_detection_parameters=load_resource(
                resource, DigitalGaugeDetection, "digitalGaugeDetectionParameters"
            ),
            dial_gauge_detection_parameters=load_resource(
                resource,
                DialGaugeDetection,
                "dialGaugeDetectionParameters",
            ),
            level_gauge_detection_parameters=load_resource(
                resource, LevelGaugeDetection, "levelGaugeDetectionParameters"
            ),
            valve_detection_parameters=load_resource(resource, ValveDetection, "valveDetectionParameters"),
        )


class VisionExtractItem(CogniteResource):
    """Data class for storing predictions for a single image file"""

    def __init__(
        self,
        file_id: int,
        predictions: VisionExtractPredictions,
        file_external_id: str | None,
        error_message: str | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.file_id = file_id
        self.file_external_id = file_external_id
        self.error_message = error_message
        self.predictions = predictions

        self._predictions_dict = predictions  # The "raw" predictions dict returned by the endpoint
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict, cognite_client: AsyncCogniteClient | None = None) -> VisionExtractItem:
        return cls(
            file_id=resource["fileId"],
            predictions=VisionExtractPredictions._load(resource["predictions"], cognite_client=cognite_client)
            if "predictions" in resource
            else VisionExtractPredictions(),
            file_external_id=resource.get("fileExternalId"),
            error_message=resource.get("errorMessage"),
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        item_dump = super().dump(camel_case=camel_case)
        item_dump["predictions"] = self.predictions.dump(camel_case=camel_case)
        return item_dump


P = ParamSpec("P")


@final
class VisionExtractJob(ContextualizationJob, Generic[P]):
    def __init__(
        self,
        job_id: int,
        status: str,
        status_time: int,
        created_time: int,
        items: list[VisionExtractItem],
        start_time: int | None,
        error_message: str | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        super().__init__(
            job_id=job_id,
            status=status,
            status_time=status_time,
            created_time=created_time,
            error_message=error_message,
            cognite_client=cognite_client,
        )
        self.items = items
        self.start_time = start_time

    def _status_path(self) -> str:
        return f"/context/vision/extract/{self.job_id}"

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> VisionExtractJob:
        return cls(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            items=[VisionExtractItem._load(item, cognite_client=cognite_client) for item in resource["items"]],
            cognite_client=cognite_client,
        )

    async def update_status_async(self) -> str:
        resource = (await self._cognite_client.vision._get(self._status_path())).json()
        self.__init__(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            items=[
                VisionExtractItem._load(item, cognite_client=self._cognite_client) for item in resource.get("items", [])
            ],
            cognite_client=self._cognite_client,
        )
        self._result = {k: v for k, v in resource.items() if k not in self._COMMON_FIELDS}
        return self.status

    @copy_doc_from_async(update_status_async)
    def update_status(self) -> str:
        return run_sync(self.update_status_async())

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        job_dump = super().dump(camel_case=camel_case)
        job_dump["items"] = [item.dump(camel_case=camel_case) for item in self.items]
        return job_dump

    def __getitem__(self, file_id: int) -> VisionExtractItem:
        """Retrieves the results for a file by id"""
        found = [item for item in self.items if item.file_id == file_id]
        if not found:
            raise IndexError(f"File with id {file_id} not found in results")
        return found[0]

    async def errors_async(self) -> list[str]:
        """Returns a list of all error messages across files"""
        results = (await self.wait_for_result())["items"]
        return [item["errorMessage"] async for item in results if "errorMessage" in item]

    @copy_doc_from_async(errors_async)
    def errors(self) -> list[str]:
        return run_sync(self.errors_async())

    def _predictions_to_annotations(
        self,
        creating_user: str,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
    ) -> list[AnnotationWrite]:
        annotations = []

        for item in self.items:
            if item.predictions is not None:
                for prediction_type, prediction_data_list in item.predictions.dump(camel_case=False).items():
                    for data in prediction_data_list:
                        # check if multiple annotations represent the same prediction (e.g. in case of dial gauges, level gauges and valves)
                        annotation_type = VISION_ANNOTATION_TYPE_MAP[prediction_type]
                        if isinstance(annotation_type, dict):
                            for key, value in annotation_type.items():
                                annotation = AnnotationWrite(
                                    annotated_resource_id=item.file_id,
                                    annotation_type=cast(AnnotationType, value),
                                    data=data[key],
                                    annotated_resource_type="file",
                                    status="suggested",
                                    creating_app=creating_app or "cognite-sdk-python",
                                    creating_app_version=creating_app_version or self._cognite_client.version,
                                    creating_user=creating_user,
                                )
                                annotations.append(annotation)
                        elif isinstance(annotation_type, str):
                            annotation = AnnotationWrite(
                                annotated_resource_id=item.file_id,
                                annotation_type=cast(AnnotationType, annotation_type),
                                data=data,
                                annotated_resource_type="file",
                                status="suggested",
                                creating_app=creating_app or "cognite-sdk-python",
                                creating_app_version=creating_app_version or self._cognite_client.version,
                                creating_user=creating_user,
                            )
                            annotations.append(annotation)
                        else:
                            raise TypeError("annotation_type must be str or dict")
        return annotations

    async def save_predictions_async(
        self,
        creating_user: str,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
    ) -> Annotation | AnnotationList:
        """
        Saves all predictions made by the feature extractors in CDF using the Annotations API.
        See https://docs.cognite.com/api/v1/#operation/annotationsSuggest

        Args:
            creating_user (str): (str, optional): A username, or email, or name.
            creating_app (str | None): The name of the app from which this annotation was created. Defaults to 'cognite-sdk-python'.
            creating_app_version (str | None): The version of the app that created this annotation. Must be a valid semantic versioning (SemVer) string. Defaults to client version.
        Returns:
            Annotation | AnnotationList: (suggested) annotation(s) stored in CDF.

        """
        if JobStatus(self.status) is JobStatus.COMPLETED:
            annotations = self._predictions_to_annotations(
                creating_user=creating_user, creating_app=creating_app, creating_app_version=creating_app_version
            )
            return await self._cognite_client.annotations.suggest(annotations=annotations if annotations else [])

        raise RuntimeError(
            "Extract job is not completed. If the job is queued or running, wait for completion and try again"
        )

    @copy_doc_from_async(save_predictions_async)
    def save_predictions(
        self,
        creating_user: str,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
    ) -> Annotation | AnnotationList:
        return run_sync(self.save_predictions_async(creating_user, creating_app, creating_app_version))


@final
class EntityMatchingPredictionResult(ContextualizationJob):
    def __init__(
        self,
        job_id: int,
        status: str,
        status_time: int,
        created_time: int,
        start_time: int,
        error_message: str | None,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        super().__init__(
            job_id=job_id,
            status=status,
            status_time=status_time,
            created_time=created_time,
            error_message=error_message,
            cognite_client=cognite_client,
        )
        self.start_time = start_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource["startTime"],
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            cognite_client=cognite_client,
        )

    def _status_path(self) -> str:
        return f"/context/entitymatching/jobs/{self.job_id}"

    async def update_status_async(self) -> str:
        """Updates the model status and returns it"""
        job_token = self.job_token
        headers = {"X-Job-Token": job_token} if job_token else {}
        resource = (await self._cognite_client.entity_matching._get(self._status_path(), headers=headers)).json()
        self.__init__(
            job_id=resource["jobId"],
            status=resource["status"],
            created_time=resource["createdTime"],
            start_time=resource["startTime"],
            status_time=resource["statusTime"],
            error_message=resource.get("errorMessage"),
            cognite_client=self._cognite_client,
        )
        self.job_token = job_token
        self._result = {k: v for k, v in resource.items() if k not in self._COMMON_FIELDS}
        return self.status

    @copy_doc_from_async(update_status_async)
    def update_status(self) -> str:
        return run_sync(self.update_status_async())


class ResourceReference(CogniteResource):
    def __init__(
        self, id: int | None = None, external_id: str | None = None, cognite_client: None | AsyncCogniteClient = None
    ) -> None:
        self.id = id
        self.external_id = external_id
        self._cognite_client = None  # Read only

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            id=resource.get("id"),
            external_id=resource.get("externalId"),
            cognite_client=cognite_client,
        )


class ResourceReferenceList(CogniteResourceList[ResourceReference], IdTransformerMixin):
    _RESOURCE = ResourceReference


class DirectionWeights(CogniteObject):
    """Direction weights for the text graph that control how far text boxes can be one from another
    in a particular direction before they are no longer connected in the same graph. Lower value means
    larger distance is allowed.

    Args:
        left (float | None): Weight for the connection towards text boxes to the left.
        right (float | None): Weight for the connection towards text boxes to the right.
        up (float | None): Weight for the connection towards text boxes above.
        down (float | None): Weight for the connection towards text boxes below.
    """

    def __init__(
        self,
        left: float | None = None,
        right: float | None = None,
        up: float | None = None,
        down: float | None = None,
    ) -> None:
        self.left = left
        self.right = right
        self.up = up
        self.down = down

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            left=resource.get("left"),
            right=resource.get("right"),
            up=resource.get("up"),
            down=resource.get("down"),
        )


class CustomizeFuzziness(CogniteObject):
    """Additional requirements for the fuzzy matching algorithm. The fuzzy match is allowed if any of these are true for each match candidate. The overall minFuzzyScore still applies, but a stricter fuzzyScore can be set here, which would not be enforced if either the minChars or maxBoxes conditions are met, making it possible to exclude detections using replacements if they are either short, or combined from many boxes.

    Args:
        fuzzy_score (float | None): The minimum fuzzy score of the candidate match.
        max_boxes (int | None): Maximum number of text boxes the potential match is composed of.
        min_chars (int | None): The minimum number of characters that must be present in the candidate match string.
    """

    def __init__(
        self,
        fuzzy_score: float | None = None,
        max_boxes: int | None = None,
        min_chars: int | None = None,
    ) -> None:
        self.fuzzy_score = fuzzy_score
        self.max_boxes = max_boxes
        self.min_chars = min_chars

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            fuzzy_score=resource.get("fuzzyScore"),
            max_boxes=resource.get("maxBoxes"),
            min_chars=resource.get("minChars"),
        )


class ConnectionFlags:
    """Connection flags for token graph. These are passed as an array of strings to the API. Only flags set to True are included in the array. There is no need to set any flags to False.

    Args:
        natural_reading_order (bool): Only connect text regions that are in natural reading order (i.e. top to bottom and left to right).
        no_text_inbetween (bool): Only connect text regions that are not separated by other text regions.
        **flags (bool): Other flags.
    """

    def __init__(
        self,
        natural_reading_order: bool = False,
        no_text_inbetween: bool = False,
        **flags: bool,
    ) -> None:
        self._flags = {
            "natural_reading_order": natural_reading_order,
            "no_text_inbetween": no_text_inbetween,
            **flags,
        }

    def dump(self, camel_case: bool = False) -> list[str]:
        return [k for k, v in self._flags.items() if v]

    @classmethod
    def load(cls, resource: list[str], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(**{flag: True for flag in resource})


class DiagramDetectConfig(CogniteObject):
    """`Configuration options for the diagrams/detect endpoint <https://api-docs.cognite.com/20230101-beta/tag/Engineering-diagrams/operation/diagramDetect/#!path=configuration&t=request>`_.

    Args:
        annotation_extract (bool | None): Read SHX text embedded in the diagram file. If present, this text will override overlapping OCR text. Cannot be used at the same time as read_embedded_text.
        case_sensitive (bool | None): Case sensitive text matching. Defaults to True.
        connection_flags (ConnectionFlags | list[str] | None): Connection flags for token graph. Two flags are supported thus far: `no_text_inbetween` and `natural_reading_order`.
        customize_fuzziness (CustomizeFuzziness | dict[str, Any] | None): Additional requirements for the fuzzy matching algorithm. The fuzzy match is allowed if any of these are true for each match candidate. The overall minFuzzyScore still applies, but a stricter fuzzyScore can be set here, which would not be enforced if either the minChars or maxBoxes conditions are met, making it possible to exclude detections using replacements if they are either short, or combined from many boxes.
        direction_delta (float | None): Maximum angle between the direction of two text boxes for them to be connected. Directions are currently multiples of 90 degrees.
        direction_weights (DirectionWeights | dict[str, Any] | None): Direction weights that control how far subsequent ocr text boxes can be from another in a particular direction and still be combined into the same detection. Lower value means larger distance is allowed. The direction is relative to the text orientation.
        min_fuzzy_score (float | None): For each detection, this controls to which degree characters can be replaced from the OCR text with similar characters, e.g. I and 1. A value of 1 will disable character replacements entirely.
        read_embedded_text (bool | None): Read text embedded in the PDF file. If present, this text will override overlapping OCR text.
        remove_leading_zeros (bool | None): Disregard leading zeroes when matching tags (e.g. "A0001" will match "A1")
        substitutions (dict[str, list[str]] | None): Override the default mapping of characters to an array of allowed substitute characters. The default mapping contains characters commonly confused by OCR. Provide your custom mapping in the format like so: {"0": ["O", "Q"], "1": ["l", "I"]}. This means: 0 (zero) is allowed to be replaced by uppercase letter O or Q, and 1 (one) is allowed to be replaced by lowercase letter l or uppercase letter I. No other replacements are allowed.
        **params (Any): Other parameters. The parameter name will be converted to camel case but the value will be passed as is.

    Example:

        Configure a call to digrams detect endpoint:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.contextualization import ConnectionFlags, DiagramDetectConfig
            >>> client = CogniteClient()
            >>> config = DiagramDetectConfig(
            ...     remove_leading_zeros=True,
            ...     connection_flags=ConnectionFlags(
            ...         no_text_inbetween=True,
            ...         natural_reading_order=True,
            ...     )
            ... )
            >>> job = client.diagrams.detect(entities=[{"name": "A1"}], file_id=123, config=config)

        If you want to use an undocumented parameter, you can pass it as a keyword argument:

            >>> config_with_undoducmented_params = DiagramDetectConfig(
            ...     remove_leading_zeros=True,
            ...     connection_flags=ConnectionFlags(new_undocumented_flag=True),
            ...     new_undocumented_param={"can_be_anything": True},
            ... )
    """

    def __init__(
        self,
        annotation_extract: bool | None = None,
        case_sensitive: bool | None = None,
        connection_flags: ConnectionFlags | list[str] | None = None,
        customize_fuzziness: CustomizeFuzziness | dict[str, Any] | None = None,
        direction_delta: float | None = None,
        direction_weights: DirectionWeights | dict[str, Any] | None = None,
        min_fuzzy_score: float | None = None,
        read_embedded_text: bool | None = None,
        remove_leading_zeros: bool | None = None,
        substitutions: dict[str, list[str]] | None = None,
        **params: Any,
    ) -> None:
        self.annotation_extract = annotation_extract
        self.case_sensitive = case_sensitive
        self.connection_flags = connection_flags
        self.customize_fuzziness = customize_fuzziness
        self.direction_delta = direction_delta
        self.direction_weights = direction_weights
        self.min_fuzzy_score = min_fuzzy_score
        self.read_embedded_text = read_embedded_text
        self.remove_leading_zeros = remove_leading_zeros
        self.substitutions = substitutions

        _known_params = {to_camel_case(k): k for k in vars(self)}
        self._extra_params = {}
        for param_name, value in params.items():
            if known := _known_params.get(to_camel_case(param_name)):
                raise ValueError(f"Provided parameter name `{param_name}` collides with a known parameter `{known}`.")
            self._extra_params[param_name] = value

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)

        for param_name, v in self._extra_params.items():
            dumped[to_camel_case(param_name) if camel_case else param_name] = v

        remove_keys: list[str] = []
        for param_name, v in dumped.items():
            if isinstance(v, CogniteObject):
                dumped[param_name] = v.dump(camel_case=camel_case)
            elif isinstance(v, ConnectionFlags):
                cf = v.dump()
                if cf:
                    dumped[param_name] = cf
                else:
                    remove_keys.append(param_name)

        for param_name in remove_keys:
            del dumped[param_name]

        return dumped

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        resource_copy = resource.copy()

        if con_flg := resource_copy.pop("connectionFlags", None):
            con_flg = ConnectionFlags.load(con_flg)
        if cus_fuz := resource_copy.pop("customizeFuzziness", None):
            cus_fuz = CustomizeFuzziness.load(cus_fuz)
        if dir_wgt := resource_copy.pop("directionWeights", None):
            dir_wgt = DirectionWeights.load(dir_wgt)

        snake_cased = convert_all_keys_to_snake_case(resource_copy)

        return cls(
            connection_flags=con_flg,
            customize_fuzziness=cus_fuz,
            direction_weights=dir_wgt,
            **snake_cased,
        )
