from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import FunctionCall, FunctionCallList, FunctionCallLog
from cognite.client.data_classes.functions import FunctionCallsFilter
from cognite.client.utils._identifier import Identifier, IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


def _get_function_internal_id(cognite_client: CogniteClient, identifier: Identifier) -> int:
    primitive = identifier.as_primitive()
    if identifier.is_id:
        return primitive

    if identifier.is_external_id:
        function = cognite_client.functions.retrieve(external_id=primitive)
        if function:
            return function.id

    raise ValueError(f'Function with external ID "{primitive}" is not found')


def _get_function_identifier(function_id: int | None, function_external_id: str | None) -> Identifier:
    identifier = IdentifierSequence.load(function_id, function_external_id, id_name="function")
    if identifier.is_singleton():
        return identifier[0]
    raise ValueError("Exactly one of function_id and function_external_id must be specified")


class FunctionCallsAPI(APIClient):
    _RESOURCE_PATH = "/functions/{}/calls"
    _RESOURCE_PATH_RESPONSE = "/functions/{}/calls/{}/response"
    _RESOURCE_PATH_LOGS = "/functions/{}/calls/{}/logs"

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
        """`List all calls associated with a specific function id. <https://developer.cognite.com/api#tag/Function-calls/operation/listFunctionCalls>`_ Either function_id or function_external_id must be specified.

        Args:
            function_id (int | None): ID of the function on which the calls were made.
            function_external_id (str | None): External ID of the function on which the calls were made.
            status (str | None): Status of the call. Possible values ["Running", "Failed", "Completed", "Timeout"].
            schedule_id (int | None): Schedule id from which the call belongs (if any).
            start_time (dict[str, int] | None): Start time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            end_time (dict[str, int] | None): End time of the call. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Maximum number of function calls to list. Pass in -1, float('inf') or None to list all Function Calls.

        Returns:
            FunctionCallList: List of function calls

        Examples:

            List function calls:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> calls = client.functions.calls.list(function_id=1)

            List function calls directly on a function object:

                >>> func = client.functions.retrieve(id=1)
                >>> calls = func.list_calls()

        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)
        filter = FunctionCallsFilter(
            status=status,
            schedule_id=schedule_id,
            start_time=start_time,
            end_time=end_time,
        ).dump(camel_case=True)
        resource_path = self._RESOURCE_PATH.format(function_id)
        return self._list(
            method="POST",
            resource_path=resource_path,
            filter=filter,
            limit=limit,
            resource_cls=FunctionCall,
            list_cls=FunctionCallList,
        )

    def retrieve(
        self,
        call_id: int,
        function_id: int | None = None,
        function_external_id: str | None = None,
    ) -> FunctionCall | None:
        """`Retrieve a single function call by id. <https://developer.cognite.com/api#tag/Function-calls/operation/byIdsFunctionCalls>`_

        Args:
            call_id (int): ID of the call.
            function_id (int | None): ID of the function on which the call was made.
            function_external_id (str | None): External ID of the function on which the call was made.

        Returns:
            FunctionCall | None: Requested function call or None if either call ID or function identifier is not found.

        Examples:

            Retrieve single function call by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> call = client.functions.calls.retrieve(call_id=2, function_id=1)

            Retrieve function call directly on a function object:

                >>> func = client.functions.retrieve(id=1)
                >>> call = func.retrieve_call(id=2)
        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)

        resource_path = self._RESOURCE_PATH.format(function_id)

        return self._retrieve_multiple(
            resource_path=resource_path,
            identifiers=IdentifierSequence.load(ids=call_id).as_singleton(),
            resource_cls=FunctionCall,
            list_cls=FunctionCallList,
        )

    def get_response(
        self,
        call_id: int,
        function_id: int | None = None,
        function_external_id: str | None = None,
    ) -> dict[str, object] | None:
        """`Retrieve the response from a function call. <https://developer.cognite.com/api#tag/Function-calls/operation/getFunctionCallResponse>`_

        Args:
            call_id (int): ID of the call.
            function_id (int | None): ID of the function on which the call was made.
            function_external_id (str | None): External ID of the function on which the call was made.

        Returns:
            dict[str, object] | None: Response from the function call.

        Examples:

            Retrieve function call response by call ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> response = client.functions.calls.get_response(call_id=2, function_id=1)

            Retrieve function call response directly on a call object:

                >>> call = client.functions.calls.retrieve(call_id=2, function_id=1)
                >>> response = call.get_response()

        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)

        resource_path = self._RESOURCE_PATH_RESPONSE.format(function_id, call_id)
        return self._get(resource_path).json().get("response")

    def get_logs(
        self,
        call_id: int,
        function_id: int | None = None,
        function_external_id: str | None = None,
    ) -> FunctionCallLog:
        """`Retrieve logs for function call. <https://developer.cognite.com/api#tag/Function-calls/operation/getFunctionCalls>`_

        Args:
            call_id (int): ID of the call.
            function_id (int | None): ID of the function on which the call was made.
            function_external_id (str | None): External ID of the function on which the call was made.

        Returns:
            FunctionCallLog: Log for the function call.

        Examples:

            Retrieve function call logs by call ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> logs = client.functions.calls.get_logs(call_id=2, function_id=1)

            Retrieve function call logs directly on a call object:

                >>> call = client.functions.calls.retrieve(call_id=2, function_id=1)
                >>> logs = call.get_logs()

        """
        identifier = _get_function_identifier(function_id, function_external_id)
        function_id = _get_function_internal_id(self._cognite_client, identifier)

        resource_path = self._RESOURCE_PATH_LOGS.format(function_id, call_id)
        return FunctionCallLog._load(self._get(resource_path).json()["items"])
