from __future__ import annotations

from datetime import date

from cognite.client.data_classes.data_modeling import DirectRelationReference, ViewId
from cognite.client.data_classes.data_modeling.typed_instances import PropertyOptions, TypedNode, TypedNodeWrite


class Person(TypedNodeWrite):
    birth_date = PropertyOptions(identifier="birthDate")

    def __init__(
        self,
        external_id: str,
        name: str,
        birth_date: date,
        email: str,
        siblings: list[DirectRelationReference] | None = None,
    ) -> None:
        super().__init__("sp_my_fixed_space", external_id, type=DirectRelationReference("sp_model_space", "person"))
        self.name = name
        self.birth_date = birth_date
        self.email = email
        self.siblings = siblings

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "view_id", "1")


class PersonRead(TypedNode[Person]):
    birth_date = PropertyOptions(identifier="birthDate")

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        name: str,
        birth_date: date,
        email: str,
        siblings: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(space, external_id, version, last_updated_time, created_time, type, deleted_time)
        self.name = name
        self.birth_date = birth_date
        self.email = email
        self.siblings = siblings

    def as_write(self) -> Person:
        return Person(self.external_id, self.name, self.birth_date, self.email, self.siblings)

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "view_id", "1")


class Asset(TypedNodeWrite):
    type_ = PropertyOptions(identifier="type")

    def __init__(self, external_id: str, name: str, type_: str) -> None:
        super().__init__("sp_my_fixed_space", external_id, type=DirectRelationReference("sp_model_space", "asset"))
        self.name = name
        self.type_ = type_

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "Asset", "1")


class TestTypedNodeWrite:
    def test_dump_person(self) -> None:
        person = Person("my_external_id", "John Doe", date(1990, 1, 1), "example@cognite.com")
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "node",
            "type": {"space": "sp_model_space", "externalId": "person"},
            "sources": [
                {
                    "source": {"space": "sp_model_space", "externalId": "view_id", "version": "1", "type": "view"},
                    "properties": {"name": "John Doe", "birthDate": "1990-01-01", "email": "example@cognite.com"},
                }
            ],
        }

        assert person.dump() == expected

    def test_dump_asset(self) -> None:
        asset = Asset("my_external_id", "My Asset", "Pump")
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "node",
            "type": {"space": "sp_model_space", "externalId": "asset"},
            "sources": [
                {
                    "source": {"space": "sp_model_space", "externalId": "Asset", "version": "1", "type": "view"},
                    "properties": {"name": "My Asset", "type": "Pump"},
                }
            ],
        }

        assert asset.dump() == expected


class TestTypedNode:
    def test_dump_load_person(self) -> None:
        person = PersonRead(
            "sp_my_fixed_space",
            "my_external_id",
            1,
            0,
            0,
            "John Doe",
            date(1990, 1, 1),
            "example@email.com",
            siblings=[
                DirectRelationReference("sp_data_space", "brother"),
                DirectRelationReference("sp_data_space", "sister"),
            ],
        )
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "node",
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {
                "sp_model_space": {
                    "view_id/1": {
                        "name": "John Doe",
                        "birthDate": "1990-01-01",
                        "email": "example@email.com",
                        "siblings": [
                            {"space": "sp_data_space", "externalId": "brother"},
                            {"space": "sp_data_space", "externalId": "sister"},
                        ],
                    },
                }
            },
        }

        assert person.dump() == expected
        loaded = PersonRead.load(expected)
        assert person == loaded
        assert isinstance(loaded.birth_date, date)
        assert all(isinstance(sibling, DirectRelationReference) for sibling in loaded.siblings or [])