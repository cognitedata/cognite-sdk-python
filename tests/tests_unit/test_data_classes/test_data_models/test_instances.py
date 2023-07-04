from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelationReference,
    EdgeApply,
    NodeApply,
    NodeOrEdgeData,
)


class TestEdgeApply:
    def test_dump(self) -> None:
        edge = EdgeApply(
            space="mySpace",
            external_id="relation:arnold_schwarzenegger:actor",
            type=DirectRelationReference("mySpace", "Person.role"),
            start_node=DirectRelationReference("mySpace", "person.external_id"),
            end_node=DirectRelationReference("mySpace", "actor.external_id"),
        )

        assert edge.dump(camel_case=True) == {
            "space": "mySpace",
            "externalId": "relation:arnold_schwarzenegger:actor",
            "type": {
                "space": "mySpace",
                "externalId": "Person.role",
            },
            "instanceType": "edge",
            "startNode": {
                "space": "mySpace",
                "externalId": "person.external_id",
            },
            "endNode": {"space": "mySpace", "externalId": "actor.external_id"},
        }


class TestNodeApply:
    def test_dump_with_snake_case_fields(self) -> None:
        # Arrange
        node = NodeApply(
            space="IntegrationTestsImmutable",
            external_id="shop:case:integration_test",
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

        # Act
        dumped = node.dump(camel_case=True)

        # Assert
        assert sorted(dumped["sources"][0]["properties"]) == sorted(
            [
                "name",
                "scenario",
                "start_time",
                "end_time",
                "cut_files",
                "bid",
                "bid_history",
                "runStatus",
                "arguments",
                "commands",
            ]
        )
