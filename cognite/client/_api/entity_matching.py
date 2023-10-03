from __future__ import annotations

from typing import Any, Sequence, TypeVar

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    ContextualizationJob,
    ContextualizationJobList,
    EntityMatchingModel,
    EntityMatchingModelList,
    EntityMatchingModelUpdate,
)
from cognite.client.utils._auxiliary import convert_true_match, is_unlimited
from cognite.client.utils._identifier import IdentifierSequence

T_ContextualizationJob = TypeVar("T_ContextualizationJob", bound=ContextualizationJob)


class EntityMatchingAPI(APIClient):
    _RESOURCE_PATH = EntityMatchingModel._RESOURCE_PATH

    def _run_job(
        self,
        job_path: str,
        job_cls: type[T_ContextualizationJob],
        json: dict[str, Any],
        status_path: str | None = None,
        headers: dict | None = None,
    ) -> T_ContextualizationJob:
        if status_path is None:
            status_path = job_path + "/"
        response = self._post(self._RESOURCE_PATH + job_path, json=json, headers=headers)

        return job_cls._load_with_status(
            data=response.json(),
            headers=response.headers,
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> EntityMatchingModel | None:
        """Retrieve model

        Args:
            id (int | None): id of the model to retrieve.
            external_id (str | None): external id of the model to retrieve.

        Returns:
            EntityMatchingModel | None: Model requested."""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=EntityMatchingModelList, resource_cls=EntityMatchingModel, identifiers=identifiers
        )

    def retrieve_multiple(
        self, ids: Sequence[int] | None = None, external_ids: Sequence[str] | None = None
    ) -> EntityMatchingModelList:
        """Retrieve models

        Args:
            ids (Sequence[int] | None): ids of the model to retrieve.
            external_ids (Sequence[str] | None): external ids of the model to retrieve.

        Returns:
            EntityMatchingModelList: Models requested."""
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=EntityMatchingModelList, resource_cls=EntityMatchingModel, identifiers=identifiers
        )

    def update(
        self,
        item: EntityMatchingModel
        | EntityMatchingModelUpdate
        | Sequence[EntityMatchingModel | EntityMatchingModelUpdate],
    ) -> EntityMatchingModelList | EntityMatchingModel:
        """Update model

        Args:
            item (EntityMatchingModel | EntityMatchingModelUpdate | Sequence[EntityMatchingModel | EntityMatchingModelUpdate]): Model(s) to update

        Returns:
            EntityMatchingModelList | EntityMatchingModel: No description."""
        return self._update_multiple(
            list_cls=EntityMatchingModelList,
            resource_cls=EntityMatchingModel,
            update_cls=EntityMatchingModelUpdate,
            items=item,
        )

    def list(
        self,
        name: str | None = None,
        description: str | None = None,
        original_id: int | None = None,
        feature_type: str | None = None,
        classifier: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> EntityMatchingModelList:
        """List models

        Args:
            name (str | None): Optional user-defined name of model.
            description (str | None): Optional user-defined description of model.
            original_id (int | None): id of the original model for models that were created with refit.
            feature_type (str | None): feature type that defines the combination of features used.
            classifier (str | None): classifier used in training.
            limit (int | None): Maximum number of items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            EntityMatchingModelList: List of models.
        """
        if is_unlimited(limit):
            limit = 1_000_000_000  # currently no pagination
        filter = {
            "originalId": original_id,
            "name": name,
            "description": description,
            "featureType": feature_type,
            "classifier": classifier,
        }
        filter = {k: v for k, v in filter.items() if v is not None}
        # NB no pagination support yet
        models = self._post(self._RESOURCE_PATH + "/list", json={"filter": filter, "limit": limit}).json()["items"]
        return EntityMatchingModelList._load(models, cognite_client=self._cognite_client)

    def list_jobs(self) -> ContextualizationJobList:
        """List jobs, typically model fit and predict runs.

        Returns:
            ContextualizationJobList: List of jobs."""
        return ContextualizationJobList._load(
            self._get(self._RESOURCE_PATH + "/jobs").json()["items"], cognite_client=self._cognite_client
        )

    def delete(self, id: int | Sequence[int] | None = None, external_id: str | Sequence[str] | None = None) -> None:
        """Delete models

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | Sequence[str] | None): External ID or list of external ids
        """

        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    def fit(
        self,
        sources: Sequence[dict | CogniteResource],
        targets: Sequence[dict | CogniteResource],
        true_matches: Sequence[dict | tuple[int | str, int | str]] | None = None,
        match_fields: dict | Sequence[tuple[str, str]] | None = None,
        feature_type: str | None = None,
        classifier: str | None = None,
        ignore_missing_fields: bool = False,
        name: str | None = None,
        description: str | None = None,
        external_id: str | None = None,
    ) -> EntityMatchingModel:
        """Fit entity matching model.

        Note:
            All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
            capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            sources (Sequence[dict | CogniteResource]): entities to match from, should have an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). Metadata fields are automatically flattened to "metadata.key" entries, such that they can be used in match_fields.
            targets (Sequence[dict | CogniteResource]): entities to match to, should have an 'id' field.  Tolerant to passing more than is needed or used.
            true_matches (Sequence[dict | tuple[int | str, int | str]] | None): Known valid matches given as a list of dicts with keys 'sourceId', 'sourceExternalId', 'targetId', 'targetExternalId'). If omitted, uses an unsupervised model. A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
            match_fields (dict | Sequence[tuple[str, str]] | None): List of (from,to) keys to use in matching. Default in the API is [('name','name')]. Also accepts {"source": .., "target": ..}.
            feature_type (str | None): feature type that defines the combination of features used, see API docs for details.
            classifier (str | None): classifier used in training.
            ignore_missing_fields (bool): whether missing data in match_fields should return error or be filled in with an empty string.
            name (str | None): Optional user-defined name of model.
            description (str | None): Optional user-defined description of model.
            external_id (str | None): Optional external id. Must be unique within the project.
        Returns:
            EntityMatchingModel: Resulting queued model."""

        if match_fields:
            match_fields_processed = [
                ft if isinstance(ft, dict) else {"source": ft[0], "target": ft[1]} for ft in match_fields
            ]
        else:
            match_fields_processed = None
        if true_matches:
            true_matches = [convert_true_match(true_match) for true_match in true_matches]
        response = self._post(
            self._RESOURCE_PATH + "/",
            json={
                "name": name,
                "description": description,
                "externalId": external_id,
                "sources": EntityMatchingModel._dump_entities(sources),
                "targets": EntityMatchingModel._dump_entities(targets),
                "trueMatches": true_matches,
                "matchFields": match_fields_processed,
                "featureType": feature_type,
                "classifier": classifier,
                "ignoreMissingFields": ignore_missing_fields,
            },
        )
        return EntityMatchingModel._load(response.json(), cognite_client=self._cognite_client)

    def predict(
        self,
        sources: Sequence[dict] | None = None,
        targets: Sequence[dict] | None = None,
        num_matches: int = 1,
        score_threshold: float | None = None,
        id: int | None = None,
        external_id: str | None = None,
    ) -> ContextualizationJob:
        """Predict entity matching.

        Warning:
            Blocks and waits for the model to be ready if it has been recently created.

        Note:
            All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
            capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            sources (Sequence[dict] | None): entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets (Sequence[dict] | None): entities to match to, does not need an 'id' field.  Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float | None): only return matches with a score above this threshold
            id (int | None): ids of the model to use.
            external_id (str | None): external ids of the model to use.

        Returns:
            ContextualizationJob: Object which can be used to wait for and retrieve results.
        """
        model = self.retrieve(id=id, external_id=external_id)
        assert model
        return model.predict(  # could call predict directly but this is friendlier
            sources=EntityMatchingModel._dump_entities(sources),
            targets=EntityMatchingModel._dump_entities(targets),
            num_matches=num_matches,
            score_threshold=score_threshold,
        )

    def refit(
        self,
        true_matches: Sequence[dict | tuple[int | str, int | str]],
        id: int | None = None,
        external_id: str | None = None,
    ) -> EntityMatchingModel:
        """Re-fits an entity matching model, using the combination of the old and new true matches.

        Note:
            All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
            capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            true_matches (Sequence[dict | tuple[int | str, int | str]]): Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId'). A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
            id (int | None): ids of the model to use.
            external_id (str | None): external ids of the model to use.
        Returns:
            EntityMatchingModel: new model refitted to true_matches."""
        model = self.retrieve(id=id, external_id=external_id)
        assert model
        return model.refit(true_matches=true_matches)
