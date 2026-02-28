from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, TypeVar

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    ContextualizationJob,
    ContextualizationJobList,
    EntityMatchingModel,
    EntityMatchingModelList,
    EntityMatchingModelUpdate,
    EntityMatchingPredictionResult,
)
from cognite.client.utils._auxiliary import convert_true_match, drop_none_values, is_unlimited, unpack_items
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

T_ContextualizationJob = TypeVar("T_ContextualizationJob", bound=ContextualizationJob)


class EntityMatchingAPI(APIClient):
    # TODO: The API class should specify the resource path, not the data class:
    _RESOURCE_PATH = EntityMatchingModel._RESOURCE_PATH

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> EntityMatchingModel | None:
        """`Retrieve model  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingRetrieve>`_

        Args:
            id (int | None): id of the model to retrieve.
            external_id (str | None): external id of the model to retrieve.

        Returns:
            EntityMatchingModel | None: Model requested.

        Examples:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> retrieved_model = client.entity_matching.retrieve(id=1)
        """

        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=EntityMatchingModelList, resource_cls=EntityMatchingModel, identifiers=identifiers
        )

    async def retrieve_multiple(
        self, ids: Sequence[int] | None = None, external_ids: SequenceNotStr[str] | None = None
    ) -> EntityMatchingModelList:
        """`Retrieve models  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingRetrieve>`_

        Args:
            ids (Sequence[int] | None): ids of the model to retrieve.
            external_ids (SequenceNotStr[str] | None): external ids of the model to retrieve.

        Returns:
            EntityMatchingModelList: Models requested.

        Examples:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> retrieved_models = client.entity_matching.retrieve_multiple([1,2,3])

        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return await self._retrieve_multiple(
            list_cls=EntityMatchingModelList, resource_cls=EntityMatchingModel, identifiers=identifiers
        )

    async def update(
        self,
        item: EntityMatchingModel
        | EntityMatchingModelUpdate
        | Sequence[EntityMatchingModel | EntityMatchingModelUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> EntityMatchingModelList | EntityMatchingModel:
        """`Update model  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingUpdate>`_

        Args:
            item (EntityMatchingModel | EntityMatchingModelUpdate | Sequence[EntityMatchingModel | EntityMatchingModelUpdate]): Model(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (EntityMatchingModel). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            EntityMatchingModelList | EntityMatchingModel: No description.

        Examples:
            >>> from cognite.client.data_classes.contextualization import EntityMatchingModelUpdate
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> client.entity_matching.update(EntityMatchingModelUpdate(id=1).name.set("New name"))
        """
        return await self._update_multiple(
            list_cls=EntityMatchingModelList,
            resource_cls=EntityMatchingModel,
            update_cls=EntityMatchingModelUpdate,
            items=item,
            mode=mode,
        )

    async def list(
        self,
        name: str | None = None,
        description: str | None = None,
        original_id: int | None = None,
        feature_type: str | None = None,
        classifier: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> EntityMatchingModelList:
        """`List models  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingModels>`_

        Args:
            name (str | None): Optional user-defined name of model.
            description (str | None): Optional user-defined description of model.
            original_id (int | None): id of the original model for models that were created with refit.
            feature_type (str | None): feature type that defines the combination of features used.
            classifier (str | None): classifier used in training.
            limit (int | None): Maximum number of items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            EntityMatchingModelList: List of models.

        Examples:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> client.entity_matching.list(limit=1, name="test")
        """
        if is_unlimited(limit):
            limit = 1_000_000_000  # currently no pagination
        filter = drop_none_values(
            {
                "originalId": original_id,
                "name": name,
                "description": description,
                "featureType": feature_type,
                "classifier": classifier,
            }
        )
        # NB no pagination support yet
        models = unpack_items(
            await self._post(
                self._RESOURCE_PATH + "/list",
                json={"filter": filter, "limit": limit},
                semaphore=self._get_semaphore("read"),
            )
        )
        return EntityMatchingModelList._load(models).set_client_ref(self._cognite_client)

    async def list_jobs(self) -> ContextualizationJobList:
        # TODO: Not in service contract
        """List jobs, typically model fit and predict runs.

        Returns:
            ContextualizationJobList: List of jobs.
        """
        return ContextualizationJobList._load(
            unpack_items(await self._get(self._RESOURCE_PATH + "/jobs", semaphore=self._get_semaphore("read")))
        ).set_client_ref(self._cognite_client)

    async def delete(
        self, id: int | Sequence[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """`Delete models  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingDelete>`_

        https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingDelete


        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
        Examples:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> client.entity_matching.delete(id=1)
        """

        await self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True
        )

    async def fit(
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
        # TODO: Not in service contract

        """Fit entity matching model.

        Note:
            All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
            capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            sources (Sequence[dict | CogniteResource]): entities to match from, should have an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). Metadata fields are automatically flattened to "metadata.key" entries, such that they can be used in match_fields.
            targets (Sequence[dict | CogniteResource]): entities to match to, should have an 'id' field. Tolerant to passing more than is needed or used.
            true_matches (Sequence[dict | tuple[int | str, int | str]] | None): Known valid matches given as a list of dicts with keys 'sourceId', 'sourceExternalId', 'targetId', 'targetExternalId'). If omitted, uses an unsupervised model. A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
            match_fields (dict | Sequence[tuple[str, str]] | None): List of (from,to) keys to use in matching. Default in the API is [('name','name')]. Also accepts {"source": .., "target": ..}.
            feature_type (str | None): feature type that defines the combination of features used, see API docs for details.
            classifier (str | None): classifier used in training.
            ignore_missing_fields (bool): whether missing data in match_fields should return error or be filled in with an empty string.
            name (str | None): Optional user-defined name of model.
            description (str | None): Optional user-defined description of model.
            external_id (str | None): Optional external id. Must be unique within the project.
        Returns:
            EntityMatchingModel: Resulting queued model.

        Example:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> sources = [{'id': 101, 'name': 'ChildAsset1', 'description': 'Child of ParentAsset1'}]
            >>> targets = [{'id': 1, 'name': 'ParentAsset1', 'description': 'Parent to ChildAsset1'}]
            >>> true_matches = [(1, 101)]
            >>> model = client.entity_matching.fit(
            ...     sources=sources,
            ...     targets=targets,
            ...     true_matches=true_matches,
            ...     description="AssetMatchingJob1"
            ... )
        """

        if match_fields:
            match_fields_processed = [
                ft if isinstance(ft, dict) else {"source": ft[0], "target": ft[1]} for ft in match_fields
            ]
        else:
            match_fields_processed = None
        if true_matches:
            true_matches = [convert_true_match(true_match) for true_match in true_matches]
        response = await self._post(
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
            semaphore=self._get_semaphore("write"),
        )
        return EntityMatchingModel._load(response.json()).set_client_ref(self._cognite_client)

    async def predict(
        self,
        sources: Sequence[dict] | None = None,
        targets: Sequence[dict] | None = None,
        num_matches: int = 1,
        score_threshold: float | None = None,
        id: int | None = None,
        external_id: str | None = None,
    ) -> EntityMatchingPredictionResult:
        """`Predict entity matching.  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingPredict>`_

        Warning:
            Blocks and waits for the model to be ready if it has been recently created.

        Note:
            All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
            capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            sources (Sequence[dict] | None): entities to match from, does not need an 'id' field. Tolerant to passing more than is needed or used (e.g. json dump of time series list). If omitted, will use data from fit.
            targets (Sequence[dict] | None): entities to match to, does not need an 'id' field. Tolerant to passing more than is needed or used. If omitted, will use data from fit.
            num_matches (int): number of matches to return for each item.
            score_threshold (float | None): only return matches with a score above this threshold
            id (int | None): id of the model to use.
            external_id (str | None): external id of the model to use.

        Returns:
            EntityMatchingPredictionResult: object which can be used to wait for and retrieve results.

        Examples:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> sources = {'id': 101, 'name': 'ChildAsset1', 'description': 'Child of ParentAsset1'}
            >>> targets = {'id': 1, 'name': 'ParentAsset1', 'description': 'Parent to ChildAsset1'}
            >>> true_matches = [(1, 101)]
            >>> model = client.entity_matching.predict(
            ...     sources = sources,
            ...     targets = targets,
            ...     num_matches = 1,
            ...     score_threshold = 0.6,
            ...     id=1
            ... )
        """
        model = await self._get_model_or_raise(id, external_id)
        # TODO: The data class should call the API class 'predict' method, not the other way around:
        return await model.predict_async(
            sources=EntityMatchingModel._dump_entities(sources),
            targets=EntityMatchingModel._dump_entities(targets),
            num_matches=num_matches,
            score_threshold=score_threshold,
        )

    async def _get_model_or_raise(self, id: int | None, external_id: str | None) -> EntityMatchingModel:
        if id is external_id is None:
            raise ValueError("Either id or external_id must be provided.")
        model = await self.retrieve(id=id, external_id=external_id)
        if model is None:
            raise ValueError("No model found with the given identifier(s).")
        return model

    async def refit(
        self,
        true_matches: Sequence[dict | tuple[int | str, int | str]],
        id: int | None = None,
        external_id: str | None = None,
    ) -> EntityMatchingModel:
        """`Re-fits an entity matching model, using the combination of the old and new true matches.  <https://api-docs.cognite.com/20230101/tag/Entity-matching/operation/entityMatchingReFit>`_

        Note:
            All users on this CDF subscription with assets read-all and entitymatching read-all and write-all
            capabilities in the project, are able to access the data sent to this endpoint.

        Args:
            true_matches (Sequence[dict | tuple[int | str, int | str]]): Updated known valid matches given as a list of dicts with keys 'fromId', 'fromExternalId', 'toId', 'toExternalId'). A tuple can be used instead of the dictionary for convenience, interpreted as id/externalId based on type.
            id (int | None): id of the model to use.
            external_id (str | None): external id of the model to use.
        Returns:
            EntityMatchingModel: new model refitted to true_matches.

        Examples:
            >>> from cognite.client import CogniteClient, AsyncCogniteClient
            >>> client = CogniteClient()
            >>> # async_client = AsyncCogniteClient()  # another option
            >>> sources = [{'id': 101, 'name': 'ChildAsset1', 'description': 'Child of ParentAsset1'}]
            >>> targets = [{'id': 1, 'name': 'ParentAsset1', 'description': 'Parent to ChildAsset1'}]
            >>> true_matches = [(1, 101)]
            >>> model = client.entity_matching.refit(true_matches=true_matches, id=1)
        """
        model = await self._get_model_or_raise(id, external_id)
        return await model.refit_async(true_matches=true_matches)
