from __future__ import annotations

from contextlib import suppress

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Function, FunctionList, FunctionSchedule, FunctionScheduleWrite, FunctionWrite
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


def handle(client, data) -> str:
    return "Hello, world!"


@pytest.fixture(scope="module")
def a_function(cognite_client: CogniteClient) -> Function:
    my_function = FunctionWrite(
        name="python-sdk-test-function",
        # Will be set by the fixture
        file_id=0,
        external_id="python-sdk-test-function",
        description="test function",
    )
    retrieved = cognite_client.functions.retrieve_multiple(
        external_ids=[my_function.external_id], ignore_unknown_ids=True
    )
    if retrieved:
        return retrieved[0]

    function = cognite_client.functions.create(
        name=my_function.name,
        external_id=my_function.external_id,
        description=my_function.description,
        function_handle=handle,
    )
    return function


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
        with pytest.raises(ValueError, match=f'Function with external ID "{xid}" is not found'):
            cognite_client.functions.schedules.create(
                function_external_id=xid,
                cron_expression="* * * * *",
                name="test_schedule",
            )

    def test_iterate_functions(self, cognite_client: CogniteClient) -> None:
        for function in cognite_client.functions:
            assert isinstance(function, Function)
            break
        else:
            assert False, "Expected at least one function"

    def test_iterate_chunked_functions(self, cognite_client: CogniteClient) -> None:
        for function in cognite_client.functions(chunk_size=2):
            assert isinstance(function, FunctionList)
            assert len(function) <= 2
            break
        else:
            assert False, "Expected at least one function"


class TestFunctionSchedulesAPI:
    def test_create_retrieve_delete(self, cognite_client: CogniteClient, a_function: Function) -> None:
        my_schedule = FunctionScheduleWrite(
            name="python-sdk-test-schedule",
            cron_expression="0 0 1 1 *",
            function_external_id=a_function.external_id,
            data={"key": "value"},
        )

        created: FunctionSchedule | None = None
        try:
            created = cognite_client.functions.schedules.create(my_schedule)

            assert created.as_write().dump() == my_schedule.dump()

            retrieved = cognite_client.functions.schedules.retrieve(id=created.id)
            assert isinstance(retrieved, FunctionSchedule)
            assert retrieved.dump() == created.dump()

            cognite_client.functions.schedules.delete(id=created.id)
        finally:
            if created:
                with suppress(CogniteAPIError):
                    cognite_client.functions.schedules.delete(id=created.id)

    def test_retrieve_unknown(self, cognite_client: CogniteClient) -> None:
        # The ID 123 should not exist
        retrieved = cognite_client.functions.schedules.retrieve(id=123)

        assert retrieved is None

    def test_retrieve_known_and_unknown(self, cognite_client: CogniteClient, a_function: Function) -> None:
        my_schedule = FunctionScheduleWrite(
            name="python-sdk-test-schedule-known",
            cron_expression="0 0 1 1 *",
            function_id=a_function.id,
            data={"key": "value"},
        )
        created: FunctionSchedule | None = None
        try:
            created = cognite_client.functions.schedules.create(my_schedule)

            retrieved = cognite_client.functions.schedules.retrieve([created.id, 123], ignore_unknown_ids=True)
            assert len(retrieved) == 1
            assert retrieved[0].id == created.id
        finally:
            if created:
                with suppress(CogniteAPIError):
                    cognite_client.functions.schedules.delete(id=created.id)

    def test_raise_retrieve_unknown(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteNotFoundError):
            cognite_client.functions.schedules.retrieve(id=[123])
