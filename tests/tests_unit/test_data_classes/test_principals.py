import pytest

from cognite.client.data_classes.principals import Principal, ServicePrincipal, UnknownPrincipal, UserPrincipal


class TestPrincipals:
    @pytest.mark.parametrize(
        "data, expected_cls",
        [
            pytest.param(
                {
                    "id": "123",
                    "name": "test_principal",
                    "type": "USER",
                    "givenName": "Test",
                    "familyName": "Principal",
                    "middleName": "Middle",
                    "pictureUrl": "http://example.com/picture.jpg",
                },
                UserPrincipal,
                id="user_principal",
            ),
            pytest.param(
                {
                    "id": "456",
                    "name": "another_principal",
                    "type": "SERVICE_ACCOUNT",
                    "createdBy": {
                        "orgId": "org_123",
                        "userId": "user_456",
                    },
                    "createdTime": 1622547800,
                    "lastUpdatedTime": 1622547800,
                    "pictureUrl": "http://example.com/service_principal.jpg",
                    "externalId": "service_principal_123",
                    "description": "A service principal for testing purposes",
                },
                ServicePrincipal,
                id="service principal",
            ),
            pytest.param(
                {
                    "id": "some_id",
                    "type": "NEW_TYPE",
                    "name": "new_name",
                    "someOtherField": {"nestedField": "value"},
                },
                UnknownPrincipal,
                id="unknown_principal",
            ),
        ],
    )
    def test_dump_load(self, data: dict[str, object], expected_cls: type[Principal]) -> None:
        loaded = Principal._load(data)

        assert isinstance(loaded, expected_cls), (
            f"Loaded principal is not of type {expected_cls.__name__}: {type(loaded).__name__}"
        )

        dumped = loaded.dump(camel_case=True)

        assert dumped == data, f"Dumped data does not match original: {dumped} != {data}"
