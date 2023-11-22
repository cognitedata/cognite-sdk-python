from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelationReference,
    EdgeApply,
    Node,
    NodeApply,
    NodeId,
    NodeOrEdgeData,
)


class TestEdgeApply:
    def test_dump_and_load(self) -> None:
        edge = EdgeApply(
            space="mySpace",
            external_id="relation:arnold_schwarzenegger:actor",
            type=DirectRelationReference("mySpace", "Person.role"),
            start_node=DirectRelationReference("mySpace", "person.external_id"),
            end_node=DirectRelationReference("mySpace", "actor.external_id"),
        )

        assert EdgeApply.load(edge.dump(camel_case=True)).dump(camel_case=True) == {
            "space": "mySpace",
            "externalId": "relation:arnold_schwarzenegger:actor",
            "type": {
                "space": "mySpace",
                "externalId": "Person.role",
            },
            "instanceType": "edge",
            "sources": [],
            "startNode": {
                "space": "mySpace",
                "externalId": "person.external_id",
            },
            "endNode": {"space": "mySpace", "externalId": "actor.external_id"},
        }


class TestNodeOrEdgeData:
    def test_direct_relation_serialization(self) -> None:
        data = NodeOrEdgeData(
            source=ContainerId("IntegrationTestsImmutable", "Case"),
            properties=dict(
                name="Integration test",
                some_direct_relation=DirectRelationReference("space", "external_id"),
                another_direct_relation_type=NodeId("space", "external_id"),
            ),
        )
        assert {
            "properties": {
                "another_direct_relation_type": {"external_id": "external_id", "space": "space"},
                "name": "Integration test",
                "some_direct_relation": {"external_id": "external_id", "space": "space"},
            },
            "source": {"external_id": "Case", "space": "IntegrationTestsImmutable", "type": "container"},
        } == data.dump(camel_case=False)


class TestNodeApply:
    def test_dump_and_load(self) -> None:
        node = NodeApply(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            type=("someSpace", "someType"),
            sources=[
                NodeOrEdgeData(
                    source=ContainerId("IntegrationTestsImmutable", "Case"),
                    properties=dict(
                        name="Integration test",
                        scenario="Integration test",
                        start_time="2021-01-01T00:00:00",
                        end_time="2021-01-01T00:00:00",
                        cut_files=["shop:cut_file:1"],
                        bid="shop:bid_matrix:8",
                        bid_history=["shop:bid_matrix:9"],
                        runStatus="Running",
                        arguments="Integration test",
                        commands={
                            "space": "IntegrationTestsImmutable",
                            "externalId": "shop:command_config:integration_test",
                        },
                    ),
                )
            ],
        )

        assert NodeApply.load(node.dump(camel_case=True)).dump(camel_case=True) == {
            "externalId": "shop:case:integration_test",
            "instanceType": "node",
            "sources": [
                {
                    "properties": {
                        "arguments": "Integration test",
                        "bid": "shop:bid_matrix:8",
                        "bid_history": ["shop:bid_matrix:9"],
                        "commands": {
                            "externalId": "shop:command_config:integration_test",
                            "space": "IntegrationTestsImmutable",
                        },
                        "cut_files": ["shop:cut_file:1"],
                        "end_time": "2021-01-01T00:00:00",
                        "name": "Integration test",
                        "runStatus": "Running",
                        "scenario": "Integration test",
                        "start_time": "2021-01-01T00:00:00",
                    },
                    "source": {"externalId": "Case", "space": "IntegrationTestsImmutable", "type": "container"},
                }
            ],
            "space": "IntegrationTestsImmutable",
            "type": {"externalId": "someType", "space": "someSpace"},
        }


class TestNode:
    def test_dump_and_load(self) -> None:
        node = Node(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
            version=1,
            type=DirectRelationReference("someSpace", "someType"),
            last_updated_time=123,
            created_time=123,
            deleted_time=None,
            properties=None,
        )

        assert Node.load(node.dump(camel_case=True)).dump(camel_case=True) == {
            "createdTime": 123,
            "externalId": "shop:case:integration_test",
            "instanceType": "node",
            "lastUpdatedTime": 123,
            "properties": {},
            "space": "IntegrationTestsImmutable",
            "type": {"externalId": "someType", "space": "someSpace"},
            "version": 1,
        }
