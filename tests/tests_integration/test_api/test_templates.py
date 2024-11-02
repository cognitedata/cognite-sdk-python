import time
import uuid

import pytest

from cognite.client.data_classes import (
    ConstantResolver,
    TemplateGroup,
    TemplateGroupList,
    TemplateGroupVersion,
    TemplateGroupVersionList,
    TemplateInstance,
    TemplateInstanceList,
)
from cognite.client.data_classes.events import Event, EventList
from cognite.client.data_classes.templates import Source, TemplateInstanceUpdate, View, ViewResolveList, ViewResolver
from cognite.client.exceptions import CogniteNotFoundError


@pytest.fixture(scope="session")
def ensure_event_test_data(cognite_client):
    events = EventList(
        [
            Event(
                external_id=f"test_evt_templates_1_{i}",
                type="test_templates_1",
                start_time=i * 1000,
            )
            for i in range(1001)
        ]
    )
    try:
        cognite_client.events.retrieve_multiple(
            external_ids=events.as_external_ids(),
            ignore_unknwown_ids=False,
        )
    except CogniteNotFoundError:
        cognite_client.events.upsert(events)
        time.sleep(3)


@pytest.fixture
def new_template_group(cognite_client):
    external_id = uuid.uuid4().hex[:20]
    username = cognite_client.iam.token.inspect().subject
    template_group = cognite_client.templates.groups.create(
        TemplateGroup(
            external_id=external_id,
            description="some description",
            owners=[username, f"{external_id}@cognite.com"],
        )
    )
    yield template_group, external_id
    cognite_client.templates.groups.delete(external_ids=external_id)
    assert cognite_client.templates.groups.retrieve_multiple(external_ids=template_group.external_id) is None


@pytest.fixture
def new_template_group_version(cognite_client, new_template_group):
    new_group, ext_id = new_template_group
    schema = """
    type Demographics @template {
        "The amount of people"
        populationSize: Int,
        "The population growth rate"
        growthRate: Float,
    }

    type Country @template {
        name: String,
        testView: Long,
        demographics: Demographics,
        deaths: TimeSeries,
        confirmed: TimeSeries,
    }"""
    version = TemplateGroupVersion(schema)
    new_version = cognite_client.templates.versions.upsert(ext_id, version=version)
    yield new_group, ext_id, new_version
    cognite_client.templates.versions.delete(ext_id, new_version.version)


@pytest.fixture
def new_template_instance(cognite_client, new_template_group_version):
    new_group, ext_id, new_version = new_template_group_version
    template_instance_1 = TemplateInstance(
        external_id="norway",
        template_name="Country",
        field_resolvers={
            "name": ConstantResolver("Norway"),
            "testView": ViewResolver("test", {"type": "bar"}),
            "demographics": ConstantResolver("norway_demographics"),
            "deaths": ConstantResolver("Norway_deaths"),
            "confirmed": ConstantResolver("Norway_confirmed"),
        },
    )
    instance = cognite_client.templates.instances.create(ext_id, new_version.version, template_instance_1)
    yield new_group, ext_id, new_version, instance
    cognite_client.templates.instances.delete(ext_id, new_version.version, instance.external_id)


@pytest.fixture
@pytest.mark.usefixtures("ensure_event_test_data")
def new_view(cognite_client, new_template_group_version):
    new_group, ext_id, new_version = new_template_group_version
    view = View(
        external_id="test",
        source=Source(
            type="events",
            filter={"startTime": {"min": "$minStartTime"}, "type": "test_templates_1"},
            mappings={"test_type": "type", "startTime": "startTime"},
        ),
    )
    view = cognite_client.templates.views.create(ext_id, new_version.version, view)
    yield new_group, ext_id, new_version, view
    try:
        cognite_client.templates.views.delete(ext_id, new_version.version, view.external_id)
    except Exception:
        None


class TestTemplatesCogniteClient:
    def test_groups_get_single(self, cognite_client, new_template_group):
        new_group, ext_id = new_template_group
        res = cognite_client.templates.groups.retrieve_multiple(external_ids=[new_group.external_id])
        assert isinstance(res[0], TemplateGroup)
        assert new_group.external_id == ext_id

    def test_groups_retrieve_unknown(self, cognite_client):
        with pytest.raises(CogniteNotFoundError):
            cognite_client.templates.groups.retrieve_multiple(external_ids=["this does not exist"])
        assert cognite_client.templates.groups.retrieve_multiple(external_ids="this does not exist") is None

    def test_groups_list_filter(self, cognite_client, new_template_group):
        new_group, ext_id = new_template_group
        res = cognite_client.templates.groups.list(owners=[f"{ext_id}@cognite.com"])
        assert len(res) == 1
        assert isinstance(res, TemplateGroupList)

    def test_groups_upsert(self, cognite_client, new_template_group):
        new_group, ext_id = new_template_group
        res = cognite_client.templates.groups.upsert(TemplateGroup(ext_id))
        assert isinstance(res, TemplateGroup)

    def test_versions_list(self, cognite_client, new_template_group_version):
        new_group, ext_id, new_version = new_template_group_version
        res = cognite_client.templates.versions.list(ext_id)
        assert len(res) == 1
        assert isinstance(res, TemplateGroupVersionList)

    def test_instances_get_single(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        res = cognite_client.templates.instances.retrieve_multiple(
            ext_id, new_version.version, new_instance.external_id
        )
        assert isinstance(res, TemplateInstance)
        assert res.external_id == new_instance.external_id

    def test_instances_list(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        res = cognite_client.templates.instances.list(ext_id, new_version.version, template_names=["Country"])
        assert isinstance(res, TemplateInstanceList)
        assert len(res) == 1

    def test_instances_upsert(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        upserted_instance = TemplateInstance(
            external_id="norway",
            template_name="Demographics",
            field_resolvers={"populationSize": ConstantResolver(5328000), "growthRate": ConstantResolver(value=0.02)},
        )
        res = cognite_client.templates.instances.upsert(ext_id, new_version.version, upserted_instance)
        assert res.external_id == new_instance.external_id

    def test_instances_update_add(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        upserted_instance = TemplateInstanceUpdate(external_id="norway").field_resolvers.add(
            {"name": ConstantResolver("Patched")}
        )
        res = cognite_client.templates.instances.update(ext_id, new_version.version, upserted_instance)
        assert res.external_id == new_instance.external_id and res.field_resolvers["name"] == ConstantResolver(
            "Patched"
        )

    def test_instances_update_remove(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        upserted_instance = TemplateInstanceUpdate(external_id="norway").field_resolvers.remove(["name"])
        res = cognite_client.templates.instances.update(ext_id, new_version.version, upserted_instance)
        assert res.external_id == new_instance.external_id and "name" not in res.field_resolvers

    def test_instances_update_set(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        upserted_instance = TemplateInstanceUpdate(external_id="norway").field_resolvers.set(
            {"name": ConstantResolver("Patched")}
        )
        res = cognite_client.templates.instances.update(ext_id, new_version.version, upserted_instance)
        assert res.external_id == new_instance.external_id and res.field_resolvers == {
            "name": ConstantResolver("Patched")
        }

    def test_query(self, cognite_client, new_template_instance):
        new_group, ext_id, new_version, new_instance = new_template_instance
        query = """
        {
        countryList
            {
                name,
                deaths {
                    externalId,
                    datapoints(limit: 2) {
                        timestamp, value
                    }
                },
                confirmed {
                    externalId,
                    datapoints(limit: 2) {
                        timestamp, value
                    }
                }
            }
        }
        """
        res = cognite_client.templates.graphql_query(ext_id, 1, query)
        assert res.data is not None

    def test_view_list(self, cognite_client, new_view):
        new_group, ext_id, new_version, view = new_view
        first_element = next(
            res
            for res in cognite_client.templates.views.list(ext_id, new_version.version)
            if res.external_id == view.external_id
        )
        assert first_element == view

    def test_view_delete(self, cognite_client, new_view):
        new_group, ext_id, new_version, view = new_view
        cognite_client.templates.views.delete(ext_id, new_version.version, [view.external_id])
        should_be_empty = [
            res
            for res in cognite_client.templates.views.list(ext_id, new_version.version)
            if res.external_id == view.external_id
        ]
        assert len(should_be_empty) == 0

    def test_view_resolve(self, cognite_client, new_view):
        new_group, ext_id, new_version, view = new_view
        res = cognite_client.templates.views.resolve(
            ext_id, new_version.version, view.external_id, input={"minStartTime": 10 * 1000}, limit=10
        )
        expected = ViewResolveList.load(
            [{"startTime": (i + 10) * 1000, "test_type": "test_templates_1"} for i in range(10)]
        )
        assert res == expected

    def test_view_resolve_pagination(self, cognite_client, new_view):
        new_group, ext_id, new_version, view = new_view
        res = cognite_client.templates.views.resolve(
            ext_id, new_version.version, view.external_id, input={"minStartTime": 0}, limit=-1
        )
        expected = ViewResolveList.load(
            [{"startTime": i, "test_type": "test_templates_1"} for i in range(0, 1_000_001, 1000)]
        )
        assert res == expected

    def test_view_upsert(self, cognite_client, new_view):
        new_group, ext_id, new_version, view = new_view
        view.source.mappings["another_type"] = "type"
        res = cognite_client.templates.views.upsert(ext_id, new_version.version, [view])
        assert res == view
