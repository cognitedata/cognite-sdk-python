from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineConfigRevisionList,
    ExtractionPipelineList,
    ExtractionPipelineRun,
    ExtractionPipelineRunFilter,
    ExtractionPipelineRunList,
    ExtractionPipelineUpdate,
)
from cognite.client.data_classes.extractionpipelines import StringFilter
from cognite.client.utils._auxiliary import handle_deprecated_camel_case_argument
from cognite.client.utils._identifier import IdentifierSequence


class ExtractionPipelinesAPI(APIClient):
    _RESOURCE_PATH = "/extpipes"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runs = ExtractionPipelineRunsAPI(*args, **kwargs)
        self.config = ExtractionPipelineConfigsAPI(*args, **kwargs)

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, identifiers=identifiers
        )

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, limit=25):
        return self._list(list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, method="GET", limit=limit)

    @overload
    def create(self, extraction_pipeline, **kwargs):
        ...

    @overload
    def create(self, extraction_pipeline, **kwargs):
        ...

    def create(self, extraction_pipeline=None, **kwargs):
        extraction_pipeline = cast(
            Union[(ExtractionPipeline, Sequence[ExtractionPipeline])],
            handle_deprecated_camel_case_argument(extraction_pipeline, "extractionPipeline", "create", kwargs),
        )
        utils._auxiliary.assert_type(extraction_pipeline, "extraction_pipeline", [ExtractionPipeline, Sequence])
        return self._create_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, items=extraction_pipeline
        )

    def delete(self, id=None, external_id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(id, external_id), wrap_ids=True, extra_body_fields={})

    @overload
    def update(self, item):
        ...

    @overload
    def update(self, item):
        ...

    def update(self, item):
        return self._update_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            update_cls=ExtractionPipelineUpdate,
            items=item,
        )


class ExtractionPipelineRunsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/runs"
    _LIST_CLASS = ExtractionPipelineRunList

    def list(self, external_id, statuses=None, message_substring=None, created_time=None, limit=25):
        if (statuses is not None) or (message_substring is not None) or (created_time is not None):
            filter = ExtractionPipelineRunFilter(
                external_id=external_id,
                statuses=statuses,
                message=StringFilter(substring=message_substring),
                created_time=created_time,
            ).dump(camel_case=True)
            return self._list(
                list_cls=ExtractionPipelineRunList,
                resource_cls=ExtractionPipelineRun,
                method="POST",
                limit=limit,
                filter=filter,
            )
        return self._list(
            list_cls=ExtractionPipelineRunList,
            resource_cls=ExtractionPipelineRun,
            method="GET",
            limit=limit,
            filter={"externalId": external_id},
        )

    @overload
    def create(self, run):
        ...

    @overload
    def create(self, run):
        ...

    def create(self, run):
        utils._auxiliary.assert_type(run, "run", [ExtractionPipelineRun, Sequence])
        return self._create_multiple(list_cls=ExtractionPipelineRunList, resource_cls=ExtractionPipelineRun, items=run)


class ExtractionPipelineConfigsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/config"
    _LIST_CLASS = ExtractionPipelineConfigRevisionList

    def retrieve(self, external_id, revision=None, active_at_time=None):
        response = self._get(
            "/extpipes/config", params={"externalId": external_id, "activeAtTime": active_at_time, "revision": revision}
        )
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)

    def list(self, external_id):
        response = self._get("/extpipes/config/revisions", params={"externalId": external_id})
        return ExtractionPipelineConfigRevisionList._load(response.json()["items"], cognite_client=self._cognite_client)

    def create(self, config):
        response = self._post("/extpipes/config", json=config.dump(camel_case=True))
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)

    def revert(self, external_id, revision):
        response = self._post("/extpipes/config/revert", json={"externalId": external_id, "revision": revision})
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)
