from typing import Any, Dict, List, Optional, Tuple, Union

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    ContextualizationJob,
    ContextualizationJobList,
    EntityMatchingModel,
    EntityMatchingModelList,
    EntityMatchingModelUpdate,
)
from cognite.client.utils._auxiliary import convert_true_match


class EntityMatchingAPI(APIClient):
    _RESOURCE_PATH = EntityMatchingModel._RESOURCE_PATH
    _LIST_CLASS = EntityMatchingModelList

    def _run_job(
        self, job_path: str, json, status_path: Optional[str] = None, headers: Dict = None, job_cls: type = None
    ) -> ContextualizationJob:
        job_cls = job_cls or ContextualizationJob
        if status_path is None:
            status_path = job_path + "/"
        return job_cls._load_with_status(
            self._post(self._RESOURCE_PATH + job_path, json=json, headers=headers).json(),
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[EntityMatchingModel]:
        """Retrieve model

        Args:
            id (int): id of the model to retrieve.
            external_id (str): external id of the model to retrieve.

        Returns:
            EntityMatchingModel: Model requested."""
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(
        self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None
    ) -> EntityMatchingModelList:
        """Retrieve models

        Args:
            ids (List[int]): ids of the model to retrieve.
            external_ids (List[str]): external ids of the model to retrieve.

        Returns:
            EntityMatchingModelList: Models requested."""
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def update(
        self,
        item: Union[
            EntityMatchingModel, EntityMatchingModelUpdate, List[Union[EntityMatchingModel, EntityMatchingModelUpdate]]
        ],
    ) -> Union[EntityMatchingModel, List[EntityMatchingModel]]:
        """Update model

        Args:
            item (Union[EntityMatchingModel,EntityMatchingModelUpdate,List[Union[EntityMatchingModel,EntityMatchingModelUpdate]]) : Model(s) to update
        """
        return self._update_multiple(items=item)

    def list(
        self,
        name: str = None,
        description: str = None,
        original_id: int = None,
        feature_type: str = None,
        classifier: str = None,
        limit=100,
    ) -> EntityMatchingModelList:
        """List models

        Args:
            name (str): Optional user-defined name of model.
            description (str): Optional user-defined description of model.
            feature_type (str): feature type that defines the combination of features used.
            classifier (str): classifier used in training.
            original_id (int): id of the original model for models that were created with refit.
            limit (int, optional): Maximum number of items to return. Defaults to 100. Set to -1, float("inf") or None to return all items.

        Returns:
            EntityMatchingModelList: List of models."""
        if limit in [None, -1, float("inf")]:
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
        return EntityMatchingModelList(
            [self._LIST_CLASS._RESOURCE._load(model, cognite_client=self._cognite_client) for model in models]
        )

    def list_jobs(self) -> ContextualizationJobList:
        """List jobs, typically model fit and predict runs.

        Returns:
            ContextualizationJobList: List of jobs."""
        return ContextualizationJobList._load(
            self._get(self._RESOURCE_PATH + "/jobs").json()["items"], cognite_client=self._cognite_client
        )

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete models

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids"""
        self._delete_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def fit(
        self,
        sources: List[Union[Dict, CogniteResource]],
        targets: List[Union[Dict, CogniteResource]],
        true_matches: List[Union[Dict, Tuple[Union[int, str], Union[int, str]]]] = None,
        match_fields: Union[Dict, List[Tuple[str, str]]] = None,
        feature_type: str = None,
        classifier: str = None,
        ignore_missing_fields: bool = False,
        name: str = None,
        description: str = None,
        external_id: str = None,
    ) -> EntityMatchingModel:
        """Fit entity matching model.
        Note: All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
        capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            sources: entities to match from, should have an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). Metadata fields are automatically flattened to "metadata.key" entries, such that they can be used in match_fields.
            targets: entities to match to, should have an 'id' field.  Tolerant to passing more than is needed or used.
            true_matches: Known valid matches given as a list of dicts with keys 'sourceId', 'sourceExternalId', 'sourceId', 'sourceExternalId'). If omitted, uses an unsupervised model.
             A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
            match_fields: List of (from,to) keys to use in matching. Default in the API is [('name','name')]. Also accepts {"source": .., "target": ..}.
            feature_type (str): feature type that defines the combination of features used, see API docs for details.
            classifier (str): classifier used in training.
            ignore_missing_fields (bool): whether missing data in match_fields should return error or be filled in with an empty string.
            name (str): Optional user-defined name of model.
            description (str): Optional user-defined description of model.
            external_id (str): Optional external id. Must be unique within the project.
        Returns:
            EntityMatchingModel: Resulting queued model."""

        if match_fields:
            match_fields = [ft if isinstance(ft, dict) else {"source": ft[0], "target": ft[1]} for ft in match_fields]
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
                "matchFields": match_fields,
                "featureType": feature_type,
                "classifier": classifier,
                "ignoreMissingFields": ignore_missing_fields,
            },
        )
        return self._LIST_CLASS._RESOURCE._load(response.json(), cognite_client=self._cognite_client)

    def predict(
        self,
        sources: Optional[List[Dict]] = None,
        targets: Optional[List[Dict]] = None,
        num_matches=1,
        score_threshold=None,
        id: Optional[int] = None,
        external_id: Optional[str] = None,
    ) -> ContextualizationJob:
        """Predict entity matching. NB. blocks and waits for the model to be ready if it has been recently created.
        Note: All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
        capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            sources: entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets: entities to match to, does not need an 'id' field.  Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float): only return matches with a score above this threshold
            ignore_missing_fields (bool): whether missing data in match_fields should be filled in with an empty string.
            id: ids of the model to use.
            external_id: external ids of the model to use.
        Returns:
            ContextualizationJob: object which can be used to wait for and retrieve results."""
        return self.retrieve(
            id=id, external_id=external_id
        ).predict(  # could call predict directly but this is friendlier
            sources=EntityMatchingModel._dump_entities(sources),
            targets=EntityMatchingModel._dump_entities(targets),
            num_matches=num_matches,
            score_threshold=score_threshold,
        )

    def refit(
        self,
        true_matches: List[Union[Dict, Tuple[Union[int, str], Union[int, str]]]],
        id: Optional[int] = None,
        external_id: Optional[str] = None,
    ) -> "EntityMatchingModel":
        """Re-fits an entity matching model, using the combination of the old and new true matches.
        Note: All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
        capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            true_matches: Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId').
                 A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
            id: ids of the model to use.
            external_id: external ids of the model to use.
        Returns:
            EntityMatchingModel: new model refitted to true_matches."""
        return self.retrieve(id=id, external_id=external_id).refit(true_matches=true_matches)
