"""
===============================================================================
e43e3fc7136649c43ca32fe9b3668ca1
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
    TransformationNotification,
    TransformationNotificationList,
    TransformationNotificationWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync


class SyncTransformationNotificationsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        destination: str | None = None,
        limit: int | None = None,
    ) -> Iterator[TransformationNotification]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        destination: str | None = None,
        limit: int | None = None,
    ) -> Iterator[TransformationNotificationList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        destination: str | None = None,
        limit: int | None = None,
    ) -> Iterator[TransformationNotification | TransformationNotificationList]:
        """
        Iterate over transformation notifications

        Args:
            chunk_size (int | None): Number of notifications to yield per chunk. Defaults to yielding notifications one by one.
            transformation_id (int | None): Filter by transformation internal numeric ID.
            transformation_external_id (str | None): Filter by transformation externalId.
            destination (str | None): Filter by notification destination.
            limit (int | None): Limits the number of results to be returned. Defaults to yielding all notifications.

        Yields:
            TransformationNotification | TransformationNotificationList: Yields notifications one by one if chunk_size is None, otherwise yields lists of notifications.
        """
        yield from SyncIterator(
            self.__async_client.transformations.notifications(
                chunk_size=chunk_size,
                transformation_id=transformation_id,
                transformation_external_id=transformation_external_id,
                destination=destination,
                limit=limit,
            )
        )  # type: ignore [misc]

    @overload
    def create(
        self, notification: TransformationNotification | TransformationNotificationWrite
    ) -> TransformationNotification: ...

    @overload
    def create(
        self, notification: Sequence[TransformationNotification] | Sequence[TransformationNotificationWrite]
    ) -> TransformationNotificationList: ...

    def create(
        self,
        notification: TransformationNotification
        | TransformationNotificationWrite
        | Sequence[TransformationNotification]
        | Sequence[TransformationNotificationWrite],
    ) -> TransformationNotification | TransformationNotificationList:
        """
        `Subscribe for notifications on the transformation errors. <https://developer.cognite.com/api#tag/Transformation-Notifications/operation/createTransformationNotifications>`_

        Args:
            notification (TransformationNotification | TransformationNotificationWrite | Sequence[TransformationNotification] | Sequence[TransformationNotificationWrite]): Notification or list of notifications to create.

        Returns:
            TransformationNotification | TransformationNotificationList: Created notification(s)

        Examples:

            Create new notifications:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationNotification
                >>> client = CogniteClient()
                >>> notifications = [TransformationNotification(transformation_id = 1, destination="my@email.com"), TransformationNotification(transformation_external_id="transformation2", destination="other@email.com"))]
                >>> res = client.transformations.notifications.create(notifications)
        """
        return run_sync(self.__async_client.transformations.notifications.create(notification=notification))

    def list(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        destination: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TransformationNotificationList:
        """
        `List notification subscriptions. <https://developer.cognite.com/api#tag/Transformation-Notifications/operation/getTransformationNotifications>`_

        Args:
            transformation_id (int | None): Filter by transformation internal numeric ID.
            transformation_external_id (str | None): Filter by transformation externalId.
            destination (str | None): Filter by notification destination.
            limit (int | None): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationNotificationList: List of transformation notifications

        Example:

            List all notifications::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> notifications_list = client.transformations.notifications.list()

            List all notifications by transformation id::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> notifications_list = client.transformations.notifications.list(transformation_id = 1)
        """
        return run_sync(
            self.__async_client.transformations.notifications.list(
                transformation_id=transformation_id,
                transformation_external_id=transformation_external_id,
                destination=destination,
                limit=limit,
            )
        )

    def delete(self, id: int | Sequence[int] | None = None) -> None:
        """
        `Deletes the specified notification subscriptions on the transformation. Does nothing when the subscriptions already don't exist <https://developer.cognite.com/api#tag/Transformation-Notifications/operation/deleteTransformationNotifications>`_

        Args:
            id (int | Sequence[int] | None): Id or list of transformation notification ids

        Examples:

            Delete schedules by id or external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.transformations.notifications.delete(id=[1,2,3])
        """
        return run_sync(self.__async_client.transformations.notifications.delete(id=id))
