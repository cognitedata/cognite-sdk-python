import os
import string

import pytest

from cognite.client.credentials import OAuthClientCertificate, OAuthClientCredentials
from cognite.client.data_classes import (
    DataSet,
    Transformation,
    TransformationDestination,
    TransformationSchedule,
    TransformationUpdate,
)
from cognite.client.data_classes.transformations import ContainsAny
from cognite.client.data_classes.transformations.common import (
    DataModelInfo,
    Edges,
    EdgeType,
    Instances,
    Nodes,
    NonceCredentials,
    OidcCredentials,
    SequenceRowsDestination,
    ViewInfo,
)
from cognite.client.utils._text import random_string
from cognite.client.utils._time import timestamp_to_ms


@pytest.fixture(scope="session")
def transform_cleanup(cognite_client):
    transforms = cognite_client.transformations.list(created_time={"max": timestamp_to_ms("1d-ago")}, limit=None)
    if transforms:
        cognite_client.transformations.delete(id=transforms.as_ids())


@pytest.fixture(scope="module")
def new_datasets(cognite_client):
    ds_ext_id1 = "transformation-ds"
    ds_ext_id2 = "transformation-ds2"
    ds1 = cognite_client.data_sets.retrieve(external_id=ds_ext_id1)
    ds2 = cognite_client.data_sets.retrieve(external_id=ds_ext_id2)
    if not ds1:
        data_set1 = DataSet(name=ds_ext_id1, external_id=ds_ext_id1)
        ds1 = cognite_client.data_sets.create(data_set1)
    if not ds2:
        data_set2 = DataSet(name=ds_ext_id2, external_id=ds_ext_id2)
        ds2 = cognite_client.data_sets.create(data_set2)
    yield ds1, ds2


@pytest.fixture
def new_transformation(cognite_client, new_datasets, transform_cleanup):
    prefix = random_string(6, string.ascii_letters)
    creds = cognite_client.config.credentials
    if not isinstance(creds, (OAuthClientCredentials, OAuthClientCertificate)):
        pytest.skip("Only run in CI environment")
    # TODO: Data Integration team, add:
    pytest.skip("Need valid credentials for: 'source_oidc_credentials' and 'destination_oidc_credentials'...")
    transform = Transformation(
        name="any",
        query="select 1",
        external_id=f"{prefix}-transformation",
        destination=TransformationDestination.assets(),
        data_set_id=new_datasets[0].id,
        source_oidc_credentials=OidcCredentials(
            client_id="invalidClientId",
            client_secret="InvalidClientSecret",
            scopes=",".join(creds.scopes),
            token_uri="InvalidTokenUrl",
            cdf_project_name=cognite_client.config.project,
        ),
        destination_oidc_credentials=OidcCredentials(
            client_id="invalidClientId",
            client_secret="InvalidClientSecret",
            scopes=",".join(creds.scopes),
            token_uri="InvalidTokenUrl",
            cdf_project_name=cognite_client.config.project,
        ),
    )
    ts = cognite_client.transformations.create(transform)

    yield ts

    cognite_client.transformations.delete(id=ts.id)
    assert cognite_client.transformations.retrieve(ts.id) is None


other_transformation = new_transformation


class TestTransformationsAPI:
    def test_create_transformation_error(self, cognite_client):
        xid = f"{random_string(6, string.ascii_letters)}-transformation"
        transform_without_name = Transformation(external_id=xid, destination=TransformationDestination.assets())

        with pytest.raises(
            ValueError, match="^External ID, name and ignore null fields are required to create a transformation."
        ):
            cognite_client.transformations.create(transform_without_name)

    def test_create_asset_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.assets()
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    @pytest.mark.skip(reason="Awaiting access to more than one CDF project for our credentials")
    def test_create_asset_with_source_destination_oidc_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.assets(),
            query="select * from _cdf.assets",
            source_oidc_credentials=OidcCredentials(
                client_id=cognite_client._config.credentials.client_id,
                client_secret=cognite_client._config.credentials.client_secret,
                scopes="https://bluefield.cognitedata.com/.default",
                token_uri="https://login.microsoftonline.com/b86328db-09aa-4f0e-9a03-0136f604d20a/oauth2/v2.0/token",
                cdf_project_name="extractor-bluefield-testing",
            ),
            destination_oidc_credentials=OidcCredentials(
                client_id=cognite_client._config.credentials.client_id,
                client_secret=cognite_client._config.credentials.client_secret,
                scopes="https://bluefield.cognitedata.com/.default",
                token_uri="https://login.microsoftonline.com/b86328db-09aa-4f0e-9a03-0136f604d20a/oauth2/v2.0/token",
                cdf_project_name="extractor-bluefield-testing2",
            ),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_raw_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.raw("myDatabase", "myTable"),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)
        assert ts.destination == TransformationDestination.raw("myDatabase", "myTable")

    def test_create_asset_hierarchy_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.asset_hierarchy()
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_string_datapoints_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.string_datapoints(),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_transformation_with_tags(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.string_datapoints(),
            tags=["vu", "hai"],
        )
        ts = cognite_client.transformations.create(transform)
        assert {"vu", "hai"} == set(ts.tags)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_instance_nodes_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        nodes = TransformationDestination.nodes(
            view=ViewInfo(
                space="test-space", external_id="testInstanceViewExternalId", version="testInstanceViewVersion"
            ),
            instance_space="test-space",
        )
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=nodes,
        )
        ts = cognite_client.transformations.create(transform)
        assert isinstance(ts.destination, Nodes)
        assert ts.destination.type == "nodes"

        assert isinstance(ts.destination.view, ViewInfo)
        assert ts.destination.view.space == "test-space"
        assert ts.destination.view.external_id == "testInstanceViewExternalId"
        assert ts.destination.view.version == "testInstanceViewVersion"

        assert ts.destination.instance_space == "test-space"

        cognite_client.transformations.delete(id=ts.id)

    def test_create_instance_edges_view_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        edges = TransformationDestination.edges(
            view=ViewInfo(
                external_id="view-testInstanceViewExternalId",
                version="view-testInstanceViewVersion",
                space="view-test-space",
            ),
            instance_space="test-instance-space",
            edge_type=None,
        )

        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=edges,
        )
        ts = cognite_client.transformations.create(transform)
        assert ts.destination.type == "edges"
        assert isinstance(ts.destination, Edges)
        assert isinstance(ts.destination.view, ViewInfo)

        assert ts.destination.view.external_id == "view-testInstanceViewExternalId"
        assert ts.destination.view.version == "view-testInstanceViewVersion"
        assert ts.destination.view.space == "view-test-space"

        assert ts.destination.instance_space == "test-instance-space"
        assert ts.destination.edge_type is None

        cognite_client.transformations.delete(id=ts.id)

    def test_create_instance_edges_type_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        edges = TransformationDestination.edges(
            view=None,
            instance_space="test-instance-space",
            edge_type=EdgeType(
                space="edge_type-space",
                external_id="edge_type-edge",
            ),
        )

        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=edges,
        )
        ts = cognite_client.transformations.create(transform)
        assert ts.destination.type == "edges"
        assert isinstance(ts.destination, Edges)

        assert ts.destination.view is None

        assert ts.destination.instance_space == "test-instance-space"
        assert isinstance(ts.destination.edge_type, EdgeType)
        assert ts.destination.edge_type.space == "edge_type-space"
        assert ts.destination.edge_type.external_id == "edge_type-edge"

        cognite_client.transformations.delete(id=ts.id)

    def test_create_instance_type_data_model_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        instances = TransformationDestination.instances(
            data_model=DataModelInfo(
                space="authorBook",
                external_id="author_book",
                version="2",
                destination_type="AuthorBook_relation",
                destination_relationship_from_type=None,
            ),
            instance_space="test-instanceSpace",
        )
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            query="SELECT * FROM my_source_table",
            destination=instances,
        )
        ts = cognite_client.transformations.create(transform)
        assert isinstance(ts.destination, Instances)
        assert ts.destination.type == "instances"

        assert isinstance(ts.destination.data_model, DataModelInfo)
        assert ts.destination.data_model.space == "authorBook"
        assert ts.destination.data_model.external_id == "author_book"
        assert ts.destination.data_model.version == "2"
        assert ts.destination.data_model.destination_type == "AuthorBook_relation"
        assert ts.destination.instance_space == "test-instanceSpace"

        cognite_client.transformations.delete(id=ts.id)

    def test_create_instance_relationship_data_model_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        instances = TransformationDestination.instances(
            data_model=DataModelInfo(
                space="authorBook",
                external_id="author_book",
                version="2",
                destination_type="AuthorBook_relation",
                destination_relationship_from_type="author_book",
            ),
            instance_space="test-instanceSpace",
        )
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            query="SELECT * FROM my_source_table",
            destination=instances,
        )
        ts = cognite_client.transformations.create(transform)
        assert isinstance(ts.destination, Instances)
        assert ts.destination.type == "instances"

        assert isinstance(ts.destination.data_model, DataModelInfo)
        assert ts.destination.data_model.space == "authorBook"
        assert ts.destination.data_model.external_id == "author_book"
        assert ts.destination.data_model.version == "2"
        assert ts.destination.data_model.destination_type == "AuthorBook_relation"
        assert ts.destination.data_model.destination_relationship_from_type == "author_book"
        assert ts.destination.instance_space == "test-instanceSpace"

        cognite_client.transformations.delete(id=ts.id)

    def test_create_sequence_rows_transformation(self, cognite_client):
        prefix = random_string(6, string.ascii_letters)
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.sequence_rows(external_id="testSequenceRows"),
        )
        ts = cognite_client.transformations.create(transform)
        assert ts.destination.type == "sequence_rows" and ts.destination.external_id == "testSequenceRows"
        cognite_client.transformations.delete(id=ts.id)

    def test_create(self, new_transformation):
        assert (
            new_transformation.name == "any"
            and new_transformation.destination == TransformationDestination.assets()
            and new_transformation.id is not None
        )

    def test_retrieve(self, cognite_client, new_transformation):
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            new_transformation.name == retrieved_transformation.name
            and new_transformation.destination == retrieved_transformation.destination
            and new_transformation.id == retrieved_transformation.id
        )

    def test_retrieve_multiple(self, cognite_client, new_transformation, other_transformation):
        retrieved_transformations = cognite_client.transformations.retrieve_multiple(
            ids=[new_transformation.id, other_transformation.id]
        )
        assert len(retrieved_transformations) == 2
        assert new_transformation.id in [
            transformation.id for transformation in retrieved_transformations
        ] and other_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_update_full(self, cognite_client, new_transformation, new_datasets):
        expected_external_id = f"m__{new_transformation.external_id}"
        new_transformation.external_id = expected_external_id
        new_transformation.name = "new name"
        new_transformation.query = "SELECT * from _cdf.assets"
        new_transformation.destination = TransformationDestination.raw("myDatabase", "myTable")
        new_transformation.data_set_id = new_datasets[1].id
        updated_transformation = cognite_client.transformations.update(new_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.external_id == retrieved_transformation.external_id == expected_external_id
            and updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
            and updated_transformation.destination == TransformationDestination.raw("myDatabase", "myTable")
            and updated_transformation.data_set_id == new_datasets[1].id
        )

    def test_update_partial(self, cognite_client, new_transformation):
        expected_external_id = f"m__{new_transformation.external_id}"
        update_transformation = (
            TransformationUpdate(id=new_transformation.id)
            .external_id.set(expected_external_id)
            .name.set("new name")
            .query.set("SELECT * from _cdf.assets")
        )
        updated_transformation = cognite_client.transformations.update(update_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.external_id == retrieved_transformation.external_id == expected_external_id
            and updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
        )

    @pytest.mark.skipif(
        os.getenv("LOGIN_FLOW") == "client_certificate", reason="Sessions do not work with client_certificate"
    )
    def test_update_nonce(self, cognite_client, new_transformation):
        session = cognite_client.iam.sessions.create()
        update_transformation = (
            TransformationUpdate(id=new_transformation.id)
            .source_nonce.set(NonceCredentials(session.id, session.nonce, cognite_client._config.project))
            .destination_nonce.set(NonceCredentials(session.id, session.nonce, cognite_client._config.project))
        )

        updated_transformation = cognite_client.transformations.update(update_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.source_session.session_id == session.id
            and updated_transformation.destination_session.session_id == session.id
            and retrieved_transformation.source_session.session_id == session.id
            and retrieved_transformation.destination_session.session_id == session.id
        )

    @pytest.mark.skipif(
        os.getenv("LOGIN_FLOW") == "client_certificate", reason="Sessions do not work with client_certificate"
    )
    def test_update_nonce_full(self, cognite_client, new_transformation):
        session = cognite_client.iam.sessions.create()
        new_transformation.source_nonce = NonceCredentials(session.id, session.nonce, cognite_client._config.project)
        new_transformation.destination_nonce = NonceCredentials(
            session.id, session.nonce, cognite_client._config.project
        )

        updated_transformation = cognite_client.transformations.update(new_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert updated_transformation.id == retrieved_transformation.id

    def test_list(self, cognite_client, new_transformation, new_datasets):
        # Filter by destination type
        retrieved_transformations = cognite_client.transformations.list(limit=None, destination_type="assets")
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

        # Filter by data set id
        retrieved_transformations = cognite_client.transformations.list(limit=None, data_set_ids=[new_datasets[0].id])
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

        # Filter by data set external id
        retrieved_transformations = cognite_client.transformations.list(
            limit=None, data_set_external_ids=[new_datasets[0].external_id]
        )
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_preview(self, cognite_client):
        query_result = cognite_client.transformations.preview(query="select 1 as id, 'asd' as name", limit=100)
        assert query_result.schema is not None
        assert query_result.results is not None
        assert len(query_result.schema) == 2
        assert len(query_result.results) == 1
        assert query_result.results[0]["id"] == 1
        assert query_result.results[0]["name"] == "asd"

    def test_preview_to_string(self, cognite_client):
        query_result = cognite_client.transformations.preview(query="select 1 as id, 'asd' as name", limit=100)
        # just make sure it doesn't raise exceptions
        str(query_result)

    def test_update_instance_nodes(self, cognite_client, new_transformation):
        new_transformation.destination = TransformationDestination.nodes(
            ViewInfo("myViewExternalId", "myViewVersion", "test-space"), "test-space"
        )
        partial_update = TransformationUpdate(id=new_transformation.id).destination.set(
            TransformationDestination.nodes(ViewInfo("myViewExternalId", "myViewVersion2", "test-space"), "test-space")
        )
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert updated_transformation.destination == TransformationDestination.nodes(
            ViewInfo("myViewExternalId", "myViewVersion", "test-space"), "test-space"
        )
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.destination == TransformationDestination.nodes(
            ViewInfo("myViewExternalId", "myViewVersion2", "test-space"), "test-space"
        )

    def test_update_instance_edges(self, cognite_client, new_transformation):
        new_transformation.destination = TransformationDestination.edges(
            instance_space="test-space", edge_type=EdgeType("edge-space", "myEdge")
        )
        partial_update = TransformationUpdate(id=new_transformation.id).destination.set(
            TransformationDestination.edges(
                instance_space="test-space",
                edge_type=EdgeType("edge-space2", "myEdge2"),
            )
        )
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert updated_transformation.destination == TransformationDestination.edges(
            None, "test-space", EdgeType("edge-space", "myEdge")
        )
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.destination == TransformationDestination.edges(
            None,
            "test-space",
            EdgeType("edge-space2", "myEdge2"),
        )

    def test_update_instance_data_model(self, cognite_client, new_transformation):
        new_transformation.destination = TransformationDestination.instances(
            DataModelInfo("authorBook", "author_book", "2", "AuthorBook_relation", None), "test-instanceSpace"
        )
        partial_update = TransformationUpdate(id=new_transformation.id).destination.set(
            TransformationDestination.instances(
                DataModelInfo("authorBook", "author_book", "2", "AuthorBook_relation", "author_book"),
                "test-instanceSpace",
            )
        )
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert updated_transformation.destination == TransformationDestination.instances(
            DataModelInfo("authorBook", "author_book", "2", "AuthorBook_relation", None), "test-instanceSpace"
        )
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.destination == TransformationDestination.instances(
            DataModelInfo("authorBook", "author_book", "2", "AuthorBook_relation", "author_book"), "test-instanceSpace"
        )

    def test_update_sequence_rows_update(self, cognite_client, new_transformation):
        new_transformation.destination = SequenceRowsDestination("myTest")
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert updated_transformation.destination == TransformationDestination.sequence_rows("myTest")

        partial_update = TransformationUpdate(id=new_transformation.id).destination.set(
            SequenceRowsDestination("myTest2")
        )
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.destination == TransformationDestination.sequence_rows("myTest2")

    def test_update_transformations_with_tags(self, cognite_client, new_transformation):
        new_transformation.tags = ["emel", "OPs"]
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert {"emel", "OPs"} == set(updated_transformation.tags)

    def test_update_transformations_with_tags_partial(self, cognite_client, new_transformation):
        partial_update = TransformationUpdate(id=new_transformation.id).tags.set(["jaime"])
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.tags == ["jaime"]
        partial_update2 = TransformationUpdate(id=new_transformation.id).tags.add(["andres", "silva"])
        partial_updated2 = cognite_client.transformations.update(partial_update2)
        assert set(partial_updated2.tags) == {"jaime", "andres", "silva"}
        partial_update3 = (
            TransformationUpdate(id=new_transformation.id).tags.add(["tharindu"]).tags.remove(["andres", "silva"])
        )
        partial_updated3 = cognite_client.transformations.update(partial_update3)
        assert set(partial_updated3.tags) == {"jaime", "tharindu"}

    def test_filter_transformations_by_tags(self, cognite_client, new_transformation, other_transformation):
        new_transformation.tags = ["hello"]
        other_transformation.tags = ["hi", "kiki"]
        cognite_client.transformations.update([new_transformation, other_transformation])
        ts = cognite_client.transformations.list(tags=ContainsAny(["hello"]))

        new_ts = ts.get(id=new_transformation.id)
        assert new_ts is not None
        assert new_ts.tags == ["hello"]

        ts3 = cognite_client.transformations.list(tags=ContainsAny(["hello", "kiki"]))
        assert len(ts3) == 2
        assert {i.id for i in ts3} == {new_transformation.id, other_transformation.id}

    def test_transformation_dump_and_str(self, cognite_client, new_transformation, new_datasets):
        cognite_client.transformations.schedules.create(
            TransformationSchedule(external_id=new_transformation.external_id, interval="* * * * *", is_paused=True)
        )
        tr: Transformation = cognite_client.transformations.retrieve(external_id=new_transformation.external_id)
        dumped = tr.dump()  # str also uses dump
        assert tr.id == dumped["id"] == new_transformation.id
        assert tr.external_id == dumped["external_id"] == new_transformation.external_id
        assert tr.name == dumped["name"] == "any"
        assert tr.query == dumped["query"] == "select 1"

        assert tr.destination.type == dumped["destination"]["type"] == "assets"
        assert tr.conflict_mode == dumped["conflict_mode"] == "upsert"
        assert tr.is_public is dumped["is_public"] is True
        assert tr.ignore_null_fields is dumped["ignore_null_fields"] is False
        assert tr.has_source_oidc_credentials is dumped["has_source_oidc_credentials"] is True
        assert tr.has_destination_oidc_credentials is dumped["has_destination_oidc_credentials"] is True
        assert tr.owner_is_current_user is dumped["owner_is_current_user"] is True
        assert tr.data_set_id == dumped["data_set_id"] == new_datasets[0].id

        schedule = dumped["schedule"]
        assert tr.schedule.id == schedule["id"] == new_transformation.id  # not the id of the schedule...
        assert tr.schedule.external_id == schedule["external_id"] == new_transformation.external_id
        assert tr.schedule.interval == schedule["interval"] == "* * * * *"
        assert tr.schedule.is_paused is schedule["is_paused"] is True
