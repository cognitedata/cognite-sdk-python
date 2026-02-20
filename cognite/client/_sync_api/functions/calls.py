"""
===============================================================================
16313d22e1182f5949139884fbbd2ad7
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import FunctionCall, FunctionCallList, FunctionCallLog
from cognite.client.utils._async_helpers import run_sync


class SyncFunctionCallsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(
        self,
        function_id: int | None = None,
        function_external_id: str | None = None,
        status: str | None = None,
        schedule_id: int | None = None,
        start_time: dict[str, int] | None = None,
        end_time: dict[str, int] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionCallList:
        """
        `List all calls associated with a specific function id. <https://developer.cognite.com/api#tag/Function-calls/operation/listFunctionCalls>`_ Either function_id or function_external_id must be specified.

        Args:
            function_id: ID of the function on which the calls were made.
            function_external_id: External ID of the function on which the calls were made.
            status: Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id: Schedule id from which the call belongs (if any).
            start_time: Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time: End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit: Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

        Returns:
            List of function calls

        Examples:

            List function calls:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> calls = client.functions.calls.list(function_id=1)

            List function calls directly on a function object:

                >>> func = client.functions.retrieve(id=1)
                >>> calls = func.list_calls()
        """
        return run_sync(
            self.__async_client.functions.calls.list(
                function_id=function_id,
                function_external_id=function_external_id,
                status=status,
                schedule_id=schedule_id,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        )

    def retrieve(
        self, call_id: int, function_id: int | None = None, function_external_id: str | None = None
    ) -> FunctionCall | None:
        """
        `Retrieve a single function call by id. <https://developer.cognite.com/api#tag/Function-calls/operation/byIdsFunctionCalls>`_

        Args:
            call_id: ID of the call.
            function_id: ID of the function on which the call was made.
            function_external_id: External ID of the function on which the call was made.

        Returns:
            Requested function call or None if either call ID or function identifier is not found.

        Examples:

            Retrieve single function call by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> call = client.functions.calls.retrieve(call_id=2, function_id=1)

            Retrieve function call directly on a function object:

                >>> func = client.functions.retrieve(id=1)
                >>> call = func.retrieve_call(id=2)
        """
        return run_sync(
            self.__async_client.functions.calls.retrieve(
                call_id=call_id, function_id=function_id, function_external_id=function_external_id
            )
        )

    def get_response(
        self, call_id: int, function_id: int | None = None, function_external_id: str | None = None
    ) -> dict[str, object] | None:
        """
        `Retrieve the response from a function call. <https://developer.cognite.com/api#tag/Function-calls/operation/getFunctionCallResponse>`_

        Args:
            call_id: ID of the call.
            function_id: ID of the function on which the call was made.
            function_external_id: External ID of the function on which the call was made.

        Returns:
            Response from the function call.

        Examples:

            Retrieve function call response by call ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> response = client.functions.calls.get_response(call_id=2, function_id=1)

            Retrieve function call response directly on a call object:

                >>> call = client.functions.calls.retrieve(call_id=2, function_id=1)
                >>> response = call.get_response()
        """
        return run_sync(
            self.__async_client.functions.calls.get_response(
                call_id=call_id, function_id=function_id, function_external_id=function_external_id
            )
        )

    def get_logs(
        self, call_id: int, function_id: int | None = None, function_external_id: str | None = None
    ) -> FunctionCallLog:
        """
        `Retrieve logs for function call. <https://developer.cognite.com/api#tag/Function-calls/operation/getFunctionCalls>`_

        Args:
            call_id: ID of the call.
            function_id: ID of the function on which the call was made.
            function_external_id: External ID of the function on which the call was made.

        Returns:
            Log for the function call.

        Examples:

            Retrieve function call logs by call ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> logs = client.functions.calls.get_logs(call_id=2, function_id=1)

            Retrieve function call logs directly on a call object:

                >>> call = client.functions.calls.retrieve(call_id=2, function_id=1)
                >>> logs = call.get_logs()
        """
        return run_sync(
            self.__async_client.functions.calls.get_logs(
                call_id=call_id, function_id=function_id, function_external_id=function_external_id
            )
        )
