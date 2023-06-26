from cognite.client import data_modeling as dm


class TestEdgeApply:
    def test_dump(self) -> None:
        edge = dm.EdgeApply(
            space="mySpace",
            external_id="relation:arnold_schwarzenegger:actor",
            type=dm.DirectRelationReference("mySpace", "Person.role"),
            start_node=dm.DirectRelationReference("mySpace", "person.external_id"),
            end_node=dm.DirectRelationReference("mySpace", "actor.external_id"),
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
