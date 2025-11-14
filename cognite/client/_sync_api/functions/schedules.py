"""
===============================================================================
dcb96d2cd929aed40477d4437416b973
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    ClientCredentials,
    FunctionSchedule,
    FunctionSchedulesList,
    TimestampRange,
)
from cognite.client.data_classes.functions import FunctionScheduleWrite
from cognite.client.utils._async_helpers import SyncIterator, run_sync


class SyncFunctionSchedulesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FunctionSchedule]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FunctionSchedulesList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = None,
    ) -> Iterator[FunctionSchedule] | Iterator[FunctionSchedulesList]:
        """
        Iterate over function schedules

        Args:
            chunk_size (int | None): The number of schedules to return in each chunk. Defaults to yielding one schedule a time.
            name (str | None): Name of the function schedule.
            function_id (int | None): ID of the function the schedules are linked to.
            function_external_id (str | None): External ID of the function the schedules are linked to.
            created_time (dict[str, int] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            cron_expression (str | None): Cron expression.
            limit (int | None): Maximum schedules to return. Defaults to return all schedules.

        Yields:
            FunctionSchedule | FunctionSchedulesList: Function schedules.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.functions.schedules(
                chunk_size=chunk_size,
                name=name,
                function_id=function_id,
                function_external_id=function_external_id,
                created_time=created_time,
                cron_expression=cron_expression,
                limit=limit,
            )
        )  # type: ignore [misc]

    @overload
    def retrieve(self, id: int, ignore_unknown_ids: bool = False) -> FunctionSchedule | None: ...

    @overload
    def retrieve(self, id: Sequence[int], ignore_unknown_ids: bool = False) -> FunctionSchedulesList: ...

    def retrieve(
        self, id: int | Sequence[int], ignore_unknown_ids: bool = False
    ) -> FunctionSchedule | None | FunctionSchedulesList:
        """
        `Retrieve a single function schedule by id. <https://developer.cognite.com/api#tag/Function-schedules/operation/byIdsFunctionSchedules>`_

        Args:
            id (int | Sequence[int]): Schedule ID
            ignore_unknown_ids (bool): Ignore IDs that are not found rather than throw an exception.

        Returns:
            FunctionSchedule | None | FunctionSchedulesList: Requested function schedule or None if not found.

        Examples:

            Get function schedule by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.functions.schedules.retrieve(id=1)
        """
        return run_sync(self.__async_client.functions.schedules.retrieve(id=id, ignore_unknown_ids=ignore_unknown_ids))

    def list(
        self,
        name: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        created_time: dict[str, int] | TimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionSchedulesList:
        """
        `List all schedules associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/listFunctionSchedules>`_

        Args:
            name (str | None): Name of the function schedule.
            function_id (int | None): ID of the function the schedules are linked to.
            function_external_id (str | None): External ID of the function the schedules are linked to.
            created_time (dict[str, int] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            cron_expression (str | None): Cron expression.
            limit (int | None): Maximum number of schedules to list. Pass in -1, float('inf') or None to list all.

        Returns:
            FunctionSchedulesList: List of function schedules

        Examples:

            List function schedules:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> schedules = client.functions.schedules.list()

            List schedules directly on a function object to get only schedules associated with this particular function:

                >>> func = client.functions.retrieve(id=1)
                >>> schedules = func.list_schedules(limit=None)
        """
        return run_sync(
            self.__async_client.functions.schedules.list(
                name=name,
                function_id=function_id,
                function_external_id=function_external_id,
                created_time=created_time,
                cron_expression=cron_expression,
                limit=limit,
            )
        )

    def create(
        self,
        name: str | FunctionScheduleWrite,
        cron_expression: str | None = None,
        function_id: int | None = None,
        function_external_id: str | None = None,
        client_credentials: dict[str, str] | ClientCredentials | None = None,
        description: str | None = None,
        data: dict[str, object] | None = None,
    ) -> FunctionSchedule:
        """
        `Create a schedule associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/postFunctionSchedules>`_

        Args:
            name (str | FunctionScheduleWrite): Name of the schedule or FunctionSchedule object. If a function schedule object is passed, the other arguments are ignored except for the client_credentials argument.
            cron_expression (str | None): Cron expression.
            function_id (int | None): Id of the function to attach the schedule to.
            function_external_id (str | None): (DEPRECATED) External id of the function to attach the schedule to.
                Note: Will be automatically converted to (internal) ID, as schedules must be bound to an ID.
            client_credentials (dict[str, str] | ClientCredentials | None): Instance of ClientCredentials
                or a dictionary containing client credentials: 'client_id' and 'client_secret'.
            description (str | None): Description of the schedule.
            data (dict[str, object] | None): Data to be passed to the scheduled run.

        Returns:
            FunctionSchedule: Created function schedule.

        Note:
            There are several ways to authenticate the function schedule — the order of priority is as follows:
                1. ``nonce`` (if provided in the ``FunctionScheduleWrite`` object)
                2. ``client_credentials`` (if provided)
                3. The credentials of *this* AsyncCogniteClient.

        Warning:
            Do not pass secrets or other confidential information via the ``data`` argument. There is a dedicated
            ``secrets`` argument in FunctionsAPI.create() for this purpose.

            Passing the reference to the Function by ``function_external_id`` is just here as a convenience to the user.
            The API require that all schedules *must* be attached to a Function by (internal) ID for authentication-
            and security purposes. This means that the lookup to get the ID is first done on behalf of the user.

        Examples:

            Create a function schedule that runs using specified client credentials (**recommended**):

                >>> import os
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ClientCredentials
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> schedule = client.functions.schedules.create(
                ...     name="My schedule",
                ...     function_id=123,
                ...     cron_expression="*/5 * * * *",
                ...     client_credentials=ClientCredentials("my-client-id", os.environ["MY_CLIENT_SECRET"]),
                ...     description="This schedule does magic stuff.",
                ...     data={"magic": "stuff"},
                ... )

            You may also create a schedule that runs with your -current- credentials, i.e. the same credentials you used
            to instantiate the ``AsyncCogniteClient`` (that you're using right now). **Note**: Unless you happen to already use
            client credentials, *this is not a recommended way to create schedules*, as it will create an explicit dependency
            on your user account, which it will run the function "on behalf of" (until the schedule is eventually removed):

                >>> schedule = client.functions.schedules.create(
                ...     name="My schedule",
                ...     function_id=456,
                ...     cron_expression="*/5 * * * *",
                ...     description="A schedule just used for some temporary testing.",
                ... )

            Create a function schedule with an oneshot session (typically used for testing purposes):

                >>> from cognite.client.data_classes.functions import FunctionScheduleWrite
                >>> session = client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
                >>> schedule = client.functions.schedules.create(
                ...     FunctionScheduleWrite(
                ...         name="My schedule",
                ...         function_id=456,
                ...         cron_expression="*/5 * * * *",
                ...         description="A schedule just used for some temporary testing.",
                ...         nonce=session.nonce
                ...     ),
                ... )
        """
        return run_sync(
            self.__async_client.functions.schedules.create(
                name=name,
                cron_expression=cron_expression,
                function_id=function_id,
                function_external_id=function_external_id,
                client_credentials=client_credentials,
                description=description,
                data=data,
            )
        )

    def delete(self, id: int) -> None:
        """
        `Delete a schedule associated with a specific project. <https://developer.cognite.com/api#tag/Function-schedules/operation/deleteFunctionSchedules>`_

        Args:
            id (int): Id of the schedule

        Examples:

            Delete function schedule:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.functions.schedules.delete(id = 123)
        """
        return run_sync(self.__async_client.functions.schedules.delete(id=id))

    def get_input_data(self, id: int) -> dict[str, object] | None:
        """
        `Retrieve the input data to the associated function. <https://developer.cognite.com/api#tag/Function-schedules/operation/getFunctionScheduleInputData>`_

        Args:
            id (int): Id of the schedule

        Returns:
            dict[str, object] | None: Input data to the associated function or None if not set. This data is passed deserialized into the function through the data argument.

        Examples:

            Get schedule input data:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.functions.schedules.get_input_data(id=123)
        """
        return run_sync(self.__async_client.functions.schedules.get_input_data(id=id))
