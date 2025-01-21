from cognite.client.data_classes.hosted_extractors import DestinationUpdate


class TestDestinations:
    def test_dump_update_obj(self) -> None:
        obj = DestinationUpdate(external_id="my-destination").target_data_set_id.set(123).credentials.set(None)
        assert obj.dump(camel_case=True) == {
            "externalId": "my-destination",
            "update": {"targetDataSetId": {"set": 123}, "credentials": {"setNull": True}},
        }
