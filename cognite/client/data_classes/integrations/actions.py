from __future__ import annotations

from abc import ABC
from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

ActionType: TypeAlias = Literal["start_task", "stop_task", "custom"]
ActionStatus: TypeAlias = Literal["pending", "running", "failed", "succeeded", "cancel_pending", "canceled"]


class ActionCore(WriteableCogniteResource["ActionWrite"], ABC):
    """An action is a request for an integration to do something outside its normal task loop,
    e.g. restart, reload config, or start/stop a task.

    The extractor polls for pending actions (through check-in) and reports the outcome back; no inbound
    connectivity is required on the extractor side.

    Args:
        external_id (str): External id of the action. Must be unique for the resource type.
        action_name (str): Name of the action to trigger. Must match a name the extractor has registered as available.
        call_metadata (dict[str, str] | None): Custom, application specific metadata passed to the extractor along with the action.
    """

    def __init__(
        self,
        external_id: str,
        action_name: str,
        call_metadata: dict[str, str] | None = None,
    ) -> None:
        self.external_id = external_id
        self.action_name = action_name
        self.call_metadata = call_metadata


class ActionWrite(ActionCore):
    """An action is a request for an integration to do something outside its normal task loop.
    This is the write/create format of the action.

    Args:
        external_id (str): External id of the action. Must be unique for the resource type.
        action_name (str): Name of the action to trigger. Must match a name the extractor has registered as available.
        call_metadata (dict[str, str] | None): Custom, application specific metadata passed to the extractor along with the action.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            action_name=resource["actionName"],
            call_metadata=resource.get("callMetadata"),
        )

    def as_write(self) -> ActionWrite:
        return self


class Action(ActionCore):
    """An action is a request for an integration to do something outside its normal task loop.
    This is the read/response format of the action.

    Args:
        external_id (str): External id of the action. Must be unique for the resource type.
        action_name (str): Name of the action to trigger. Must match a name the extractor has registered as available.
        status (ActionStatus): Current status of the action.
        created_time (int): The time when this action was created, in milliseconds since epoch.
        last_updated_time (int): The time when this action was last updated, in milliseconds since epoch.
        call_metadata (dict[str, str] | None): Custom, application specific metadata passed to the extractor along with the action.
        result_message (str | None): Message reported by the extractor when the action completed or failed.
        result_metadata (dict[str, str] | None): Custom, application specific metadata reported by the extractor when the action completed or failed.
    """

    def __init__(
        self,
        external_id: str,
        action_name: str,
        status: ActionStatus,
        created_time: int,
        last_updated_time: int,
        call_metadata: dict[str, str] | None = None,
        result_message: str | None = None,
        result_metadata: dict[str, str] | None = None,
    ) -> None:
        super().__init__(external_id=external_id, action_name=action_name, call_metadata=call_metadata)
        self.status = status
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.result_message = result_message
        self.result_metadata = result_metadata

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            action_name=resource["actionName"],
            status=resource["status"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            call_metadata=resource.get("callMetadata"),
            result_message=resource.get("resultMessage"),
            result_metadata=resource.get("resultMetadata"),
        )

    def as_write(self) -> ActionWrite:
        """Returns this Action as an ActionWrite"""
        return ActionWrite(external_id=self.external_id, action_name=self.action_name, call_metadata=self.call_metadata)

    def __hash__(self) -> int:
        return hash(self.external_id)


class ActionWriteList(CogniteResourceList[ActionWrite], ExternalIDTransformerMixin):
    _RESOURCE = ActionWrite


class ActionList(WriteableCogniteResourceList[ActionWrite, Action], ExternalIDTransformerMixin):
    _RESOURCE = Action

    def as_write(self) -> ActionWriteList:
        return ActionWriteList([item.as_write() for item in self.data])
