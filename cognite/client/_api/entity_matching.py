from typing import TypeVar

from cognite.client._api_client import APIClient
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

    def _run_job(self, job_path, job_cls, json, status_path=None, headers=None):
        if status_path is None:
            status_path = job_path + "/"
        return job_cls._load_with_status(
            self._post((self._RESOURCE_PATH + job_path), json=json, headers=headers).json(),
            status_path=(self._RESOURCE_PATH + status_path),
            cognite_client=self._cognite_client,
        )

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=EntityMatchingModelList, resource_cls=EntityMatchingModel, identifiers=identifiers
        )

    def retrieve_multiple(self, ids=None, external_ids=None):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=EntityMatchingModelList, resource_cls=EntityMatchingModel, identifiers=identifiers
        )

    def update(self, item):
        return self._update_multiple(
            list_cls=EntityMatchingModelList,
            resource_cls=EntityMatchingModel,
            update_cls=EntityMatchingModelUpdate,
            items=item,
        )

    def list(self, name=None, description=None, original_id=None, feature_type=None, classifier=None, limit=100):
        if is_unlimited(limit):
            limit = 1000000000
        filter = {
            "originalId": original_id,
            "name": name,
            "description": description,
            "featureType": feature_type,
            "classifier": classifier,
        }
        filter = {k: v for (k, v) in filter.items() if (v is not None)}
        models = self._post((self._RESOURCE_PATH + "/list"), json={"filter": filter, "limit": limit}).json()["items"]
        return EntityMatchingModelList(
            [EntityMatchingModel._load(model, cognite_client=self._cognite_client) for model in models]
        )

    def list_jobs(self):
        return ContextualizationJobList._load(
            self._get(self._RESOURCE_PATH + "/jobs").json()["items"], cognite_client=self._cognite_client
        )

    def delete(self, id=None, external_id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    def fit(
        self,
        sources,
        targets,
        true_matches=None,
        match_fields=None,
        feature_type=None,
        classifier=None,
        ignore_missing_fields=False,
        name=None,
        description=None,
        external_id=None,
    ):
        if match_fields:
            match_fields_processed = [
                (ft if isinstance(ft, dict) else {"source": ft[0], "target": ft[1]}) for ft in match_fields
            ]
        else:
            match_fields_processed = None
        if true_matches:
            true_matches = [convert_true_match(true_match) for true_match in true_matches]
        response = self._post(
            (self._RESOURCE_PATH + "/"),
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

    def predict(self, sources=None, targets=None, num_matches=1, score_threshold=None, id=None, external_id=None):
        model = self.retrieve(id=id, external_id=external_id)
        assert model
        return model.predict(
            sources=EntityMatchingModel._dump_entities(sources),
            targets=EntityMatchingModel._dump_entities(targets),
            num_matches=num_matches,
            score_threshold=score_threshold,
        )

    def refit(self, true_matches, id=None, external_id=None):
        model = self.retrieve(id=id, external_id=external_id)
        assert model
        return model.refit(true_matches=true_matches)
