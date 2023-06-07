from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId


class TestContainerReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId", "type": "container"}

        assert data == ContainerId.load(data).dump(camel_case=True)


class TestViewReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId", "version": "myVersion", "type": "view"}

        assert data == ViewId.load(data).dump(camel_case=True)
