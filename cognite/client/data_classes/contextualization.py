import time
from typing import Any, Dict, List, Optional, Tuple, Union

from cognite.client.data_classes._base import (
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.exceptions import ModelFailedException
from cognite.client.utils._auxiliary import convert_true_match


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

    def __init__(
        self,
        job_id=None,
        status=None,
        error_message=None,
        created_time=None,
        start_time=None,
        status_time=None,
        status_path=None,
        cognite_client=None,
        **kwargs,
    ):
        """Data class for the result of a contextualization job. All keys in the body become snake-cased variables in the class (e.g. `items`, `svg_url`)"""
        self.job_id = job_id
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.status_time = status_time
        self.error_message = error_message
        self._cognite_client = cognite_client
        self._result = None
        self._status_path = status_path

    def update_status(self) -> str:
        """Updates the model status and returns it"""
        data = self._cognite_client.entity_matching._get(f"{self._status_path}{self.job_id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        self._result = {k: v for k, v in data.items() if k not in self._COMMON_FIELDS}
        return self.status

    def wait_for_completion(self, interval=1):
        """Waits for job completion, raising ModelFailedException if fit failed - generally not needed to call as it is called by result"""
        while True:
            self.update_status()
            if self.status not in ["Queued", "Running"]:
                break
            time.sleep(interval)
        if self.status == "Failed":
            raise ModelFailedException(self.__class__.__name__, self.job_id, self.error_message)

    @property
    def result(self):
        """Waits for the job to finish and returns the results."""
        if not self._result:
            self.wait_for_completion()
        return self._result

    def __str__(self):
        return "%s(id: %d,status: %s,error: %s)" % (
            self.__class__.__name__,
            self.job_id,
            self.status,
            self.error_message,
        )

    @classmethod
    def _load_with_status(cls, data, status_path, cognite_client):
        obj = cls._load(data, cognite_client=cognite_client)
        obj._status_path = status_path
        return obj


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
        cognite_client=None,
        classifier=None,
        feature_type=None,
        match_fields=None,
        model_type=None,
        name=None,
        description=None,
        external_id=None,
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
        return "%s(id: %d,status: %s,error: %s)" % (self.__class__.__name__, self.id, self.status, self.error_message)

    def update_status(self) -> str:
        """Updates the model status and returns it"""
        data = self._cognite_client.entity_matching._get(f"{self._STATUS_PATH}{self.id}").json()
        self.status = data["status"]
        self.status_time = data.get("statusTime")
        self.start_time = data.get("startTime")
        self.created_time = self.created_time or data.get("createdTime")
        self.error_message = data.get("errorMessage")
        return self.status

    def wait_for_completion(self, interval=1):
        """Waits for model completion, raising ModelFailedException if fit failed - generally not needed to call as it is called by predict"""
        while True:
            self.update_status()
            if self.status not in ["Queued", "Running"]:
                break
            time.sleep(interval)
        if self.status == "Failed":
            raise ModelFailedException(self.__class__.__name__, self.id, self.error_message)

    def predict(
        self,
        sources: Optional[List[Dict]] = None,
        targets: Optional[List[Dict]] = None,
        num_matches=1,
        score_threshold=None,
    ) -> ContextualizationJob:
        """Predict entity matching. NB. blocks and waits for the model to be ready if it has been recently created.

        Args:
            sources: entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets: entities to match to, does not need an 'id' field.  Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float): only return matches with a score above this threshold
            ignore_missing_fields (bool): whether missing data in keyFrom or keyTo should be filled in with an empty string.

        Returns:
            ContextualizationJob: object which can be used to wait for and retrieve results."""
        self.wait_for_completion()
        return self._cognite_client.entity_matching._run_job(
            job_path=f"/predict",
            status_path=f"/jobs/",
            id=self.id,
            sources=self.dump_entities(sources),
            targets=self.dump_entities(targets),
            num_matches=num_matches,
            score_threshold=score_threshold,
        )

    def refit(self, true_matches: List[Union[Dict, Tuple[Union[int, str], Union[int, str]]]]) -> "EntityMatchingModel":
        """Re-fits an entity matching model, using the combination of the old and new true matches.

        Args:
            true_matches: Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId').
                 A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
        Returns:
            EntityMatchingModel: new model refitted to true_matches."""
        true_matches = [convert_true_match(true_match) for true_match in true_matches]
        self.wait_for_completion()
        response = self._cognite_client.entity_matching._camel_post(
            f"/refit", json={"trueMatches": true_matches, "id": self.id}
        )
        return self._load(response.json(), cognite_client=self._cognite_client)

    @staticmethod
    def dump_entities(entities: List[Union[Dict, CogniteResource]]) -> Optional[List[Dict]]:
        if entities:
            return [
                {k: v for k, v in e.dump(camel_case=True).items() if isinstance(v, str) or k == "id"}
                if isinstance(e, CogniteResource)
                else e
                for e in entities
            ]


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
    def name(self):
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "name")

    @property
    def description(self):
        return EntityMatchingModelUpdate._PrimitiveUpdate(self, "description")


class EntityMatchingModelList(CogniteResourceList):
    _RESOURCE = EntityMatchingModel
    _UPDATE = EntityMatchingModelUpdate
