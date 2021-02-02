import uuid

import pytest

from cognite.client.alpha import CogniteClient
from cognite.client.data_classes import TemplateGroup, TemplateGroupList, TemplateGroupVersion, TemplateInstance
from cognite.client.exceptions import CogniteNotFoundError

API = CogniteClient()
API_GROUPS = API.templates.groups


@pytest.fixture
def new_template_group():
    external_id = uuid.uuid4().hex[0:20]

    template_group = API_GROUPS.create(
        TemplateGroup(external_id=external_id, description="some description", owners=[external_id + "@cognite.com"])
    )
    yield template_group, external_id
    API_GROUPS.delete(external_ids=external_id)
    assert API_GROUPS.retrieve_multiple(external_ids=template_group.external_id) is None


class TestTemplatesAPI:
    def test_get_single_group(self, new_template_group):
        new_group, ext_id = new_template_group
        res = API_GROUPS.retrieve_multiple(external_ids=[new_group.external_id])
        assert isinstance(res[0], TemplateGroup)
        assert new_group.external_id == ext_id

    def test_retrieve_unknown(self, new_template_group):
        with pytest.raises(CogniteNotFoundError):
            API_GROUPS.retrieve_multiple(external_ids=["this does not exist"])
        assert API_GROUPS.retrieve_multiple(external_ids="this does not exist") is None

    def test_list_filter(self, new_template_group):
        new_group, ext_id = new_template_group
        res = API_GROUPS.list(owners=[ext_id + "@cognite.com"])
        assert len(res) == 1
        assert isinstance(res, TemplateGroupList)

    def test_upsert(self, new_template_group):
        new_group, ext_id = new_template_group
        res = API_GROUPS.upsert(TemplateGroup(ext_id))
        assert isinstance(res, TemplateGroup)
