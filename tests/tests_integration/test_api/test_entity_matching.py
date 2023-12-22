import random

import pytest

from cognite.client.data_classes import (
    ContextualizationJob,
    EntityMatchingModel,
    EntityMatchingModelList,
    EntityMatchingModelUpdate,
)
from cognite.client.data_classes.contextualization import ContextualizationJobList
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture
def fitted_model(cognite_client):
    extid = "abc" + str(random.randint(1, 1000000000))
    try:
        cognite_client.entity_matching.delete(external_id=extid)
    except CogniteAPIError as e:
        if e.code != 404:
            raise
    entities_from = [{"id": 1, "name": "xx-yy"}]
    entities_to = [{"id": 2, "bloop": "yy"}, {"id": 3, "bloop": "zz"}]
    model = cognite_client.entity_matching.fit(
        sources=entities_from,
        targets=entities_to,
        true_matches=[{"sourceId": 1, "targetId": 2}],
        feature_type="bigram",
        match_fields=[("name", "bloop")],
        external_id=extid,
    )
    yield model
    cognite_client.entity_matching.delete(id=model.id)


class TestEntityMatchingIntegration:
    def test_fit_retrieve_update(self, cognite_client, fitted_model):
        assert isinstance(fitted_model, EntityMatchingModel)
        assert "Queued" == fitted_model.status

        job = fitted_model.predict(sources=[{"name": "foo-bar"}], targets=[{"bloop": "foo-42"}])
        assert "Completed" == fitted_model.status
        assert isinstance(job, ContextualizationJob)
        assert "Queued" == job.status
        assert {"matches", "source"} == set(job.result["items"][0].keys()) - {"matchFrom"}
        assert "Completed" == job.status

        job = fitted_model.predict()
        assert isinstance(job, ContextualizationJob)
        assert "Queued" == job.status
        assert {"matches", "source"} == set(job.result["items"][0].keys()) - {"matchFrom"}
        assert "Completed" == job.status

        # Retrieve model
        model = cognite_client.entity_matching.retrieve(id=fitted_model.id)
        assert model.classifier == "randomforest"
        assert model.feature_type == "bigram"
        assert model.match_fields == [{"source": "name", "target": "bloop"}]

        # Retrieve models
        models = cognite_client.entity_matching.retrieve_multiple(ids=[model.id, model.id])
        assert 2 == len(models)
        assert model == models[0]
        assert model == models[1]

        # Update model
        model.name = "new_name"
        updated_model = cognite_client.entity_matching.update(model)
        assert type(updated_model) == EntityMatchingModel
        assert updated_model.name == "new_name"

        updated_model2 = cognite_client.entity_matching.update(
            EntityMatchingModelUpdate(id=model.id).description.set("new description")
        )
        assert type(updated_model2) == EntityMatchingModel
        assert updated_model2.description == "new description"

    def test_refit(self, cognite_client, fitted_model):
        new_model = fitted_model.refit(true_matches=[(1, 3)])
        assert new_model.id is not None
        assert new_model.id != fitted_model.id
        assert "Completed" == fitted_model.status
        assert isinstance(new_model, EntityMatchingModel)
        assert "Queued" == new_model.status

        job = new_model.predict(sources=[{"name": "foo-bar"}], targets=[{"bloop": "foo-42"}])
        assert {"matches", "source"} == set(job.result["items"][0].keys()) - {"matchFrom"}
        assert "Completed" == job.status
        cognite_client.entity_matching.delete(id=new_model.id)

    def test_true_match_formats(self, cognite_client):
        entities_from = [{"id": 1, "name": "xx-yy"}]
        entities_to = [{"id": 2, "name": "yy"}, {"id": 3, "externalId": "aa", "name": "xx"}]
        model = cognite_client.entity_matching.fit(
            sources=entities_from, targets=entities_to, true_matches=[{"sourceId": 1, "targetExternalId": "aa"}, (1, 2)]
        )
        assert isinstance(model, EntityMatchingModel)
        assert "Queued" == model.status
        cognite_client.entity_matching.delete(id=model.id)

    def test_extra_options(self, cognite_client):
        entities_from = [{"id": 1, "name": "xx-yy"}]
        entities_to = [{"id": 2, "name": "yy"}, {"id": 3, "name": "xx", "missing": "yy"}]
        model = cognite_client.entity_matching.fit(
            sources=entities_from,
            targets=entities_to,
            true_matches=[(1, 2)],
            feature_type="bigram",
            match_fields=[("name", "missing")],
            ignore_missing_fields=True,
            classifier="LogisticRegression",
            name="my_bigram_logReg_model",
            description="My model with bigram features",
        )
        assert isinstance(model, EntityMatchingModel)
        assert "Queued" == model.status
        job = model.predict()
        assert {"matches", "source"} == set(job.result["items"][0].keys()) - {"matchFrom"}

        cognite_client.entity_matching.delete(id=model.id)

    def test_list(self, cognite_client):
        models_list = cognite_client.entity_matching.list()
        assert len(models_list) > 0
        assert type(models_list) == EntityMatchingModelList
        assert all([type(x) == EntityMatchingModel for x in models_list])
        # Add filter
        models_list = cognite_client.entity_matching.list(feature_type="bigram")
        assert {model.feature_type for model in models_list} == {"bigram"}

    @pytest.mark.skip("extremely slow due to lack of paging")
    def test_list_jobs(self, cognite_client):
        jobs_list = cognite_client.entity_matching.list_jobs()
        assert len(jobs_list) > 0
        assert type(jobs_list) == ContextualizationJobList
        assert all([type(x) == ContextualizationJob for x in jobs_list])

    def test_direct_predict(self, cognite_client, fitted_model):
        job = cognite_client.entity_matching.predict(external_id=fitted_model.external_id)
        job2 = cognite_client.entity_matching.predict(id=fitted_model.id)
        assert isinstance(job, ContextualizationJob)
        assert "Queued" == job.status
        assert isinstance(job2, ContextualizationJob)

    def test_direct_refit(self, cognite_client, fitted_model):
        new_model = cognite_client.entity_matching.refit(external_id=fitted_model.external_id, true_matches=[(1, 3)])
        new_model2 = cognite_client.entity_matching.refit(id=fitted_model.id, true_matches=[(1, 3)])
        assert isinstance(new_model, EntityMatchingModel)
        assert isinstance(new_model2, EntityMatchingModel)
        cognite_client.entity_matching.delete(id=[new_model.id, new_model2.id])
