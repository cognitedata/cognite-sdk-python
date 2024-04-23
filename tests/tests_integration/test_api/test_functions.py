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

    def test_function_list_schedules_unlimited(self, cognite_client: CogniteClient):
        expected_unique_schedules = 5
        # This is an integration test dummy function that purposefully doesn't have an external id.
        fn = cognite_client.functions.retrieve(id=2495645514618888)
        schedules = fn.list_schedules(limit=-1)

        assert len(schedules) == expected_unique_schedules
        assert len({s.id for s in schedules}) == expected_unique_schedules
        assert len({s.cron_expression for s in schedules}) == expected_unique_schedules

    def test_create_schedule_with_bad_external_id(self, cognite_client: CogniteClient):
        xid = "bad_xid"
        with pytest.raises(ValueError, match=f"Function with external ID {xid} is not found"):
            cognite_client.functions.schedules.create(
                external_id=xid,
                cron_expression="* * * * *",
                name="test_schedule",
            )
