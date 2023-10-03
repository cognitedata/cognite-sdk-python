from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteNotFoundError


class TestFunctionsAPI:
    def test_retrieve_unknown_raises_error(self, cognite_client: CogniteClient):
        with pytest.raises(CogniteNotFoundError) as e:
            cognite_client.functions.retrieve_multiple(external_ids=["this does not exist"])

        assert e.value.not_found[0]["external_id"] == "this does not exist"

    def test_retrieve_unknown_ignore_unknowns(self, cognite_client: CogniteClient):
        res = cognite_client.functions.retrieve_multiple(external_ids=["this does not exist"], ignore_unknown_ids=True)
        assert len(res) == 0
