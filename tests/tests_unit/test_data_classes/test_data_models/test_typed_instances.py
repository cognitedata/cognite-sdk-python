from datetime import date

from cognite.client.data_classes.data_modeling import DirectRelationReference, ViewId
from cognite.client.data_classes.data_modeling.typed_instances import PropertyOptions, TypedNodeWrite


class Person(TypedNodeWrite):
    birth_date = PropertyOptions(identifier="birthDate")

    def __init__(self, external_id: str, name: str, birth_date: date, email: str) -> None:
        super().__init__("sp_my_fixed_space", external_id, type=DirectRelationReference("sp_model_space", "person"))
        self.name = name
        self.birth_date = birth_date
        self.email = email

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "view_id", "1")


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
