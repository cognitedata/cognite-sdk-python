from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message
from cognite.client.data_classes.agents.chat import (
    ActionCall,
    AgentChatResponse,
    ClientToolAction,
    ClientToolCall,
    ClientToolResult,
    TextContent,
    UnknownActionCall,
)


@pytest.fixture
def action_call_response_body() -> dict:
    """Response body when agent requests an action."""
    return {
        "agentId": "my_agent",
        "agentExternalId": "my_agent",
        "response": {
            "cursor": "cursor_12345",
            "messages": [
                {
                    "content": {"text": "", "type": "text"},
                    "actions": [
                        {
                            "type": "clientTool",
                            "actionId": "call_abc123",
                            "clientTool": {
                                "name": "add",
                                "arguments": '{"a": 42, "b": 58}',
                            },
                        }
                    ],
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


@pytest.fixture
def final_response_body() -> dict:
    """Final response after action execution."""
    return {
        "agentId": "my_agent",
        "agentExternalId": "my_agent",
        "response": {
            "cursor": "cursor_67890",
            "messages": [
                {
                    "content": {"text": "The result is 100.", "type": "text"},
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


class TestClientToolCall:
    def test_dump_serializes_arguments_to_json(self) -> None:
        call = ClientToolCall(
            action_id="call_456",
            name="multiply",
            arguments={"x": 5, "y": 7},
        )

        dumped = call.dump(camel_case=True)

        assert dumped["type"] == "clientTool"
        assert dumped["actionId"] == "call_456"
        assert dumped["clientTool"]["name"] == "multiply"
        # Arguments should be JSON string
        import json

        assert json.loads(dumped["clientTool"]["arguments"]) == {"x": 5, "y": 7}


class TestActionCallPolymorphism:
    def test_unknown_action_call_loaded_for_unknown_type(self) -> None:
        data = {
            "type": "unknownActionType",
            "actionId": "call_999",
            "someField": "someValue",
        }

        call = ActionCall._load(data)

        assert isinstance(call, UnknownActionCall)
        assert call.action_id == "call_999"
        assert call.type == "unknownActionType"


class TestClientToolResult:
    def test_init_with_message_content(self) -> None:
        text_content = TextContent(text="Result: 100")
        result = ClientToolResult(
            action_id="call_456",
            content=text_content,
        )

        assert result.action_id == "call_456"
        assert result.content is text_content
        assert result.content.text == "Result: 100"

    def test_load(self) -> None:
        data = {
            "type": "clientTool",
            "actionId": "call_abc",
            "role": "action",
            "content": {"text": "Done", "type": "text"},
            "data": [],
        }

        result = ClientToolResult._load_result(data)

        assert isinstance(result, ClientToolResult)
        assert result.action_id == "call_abc"
        assert result.content.text == "Done"


class TestChatWithActions:
    def test_chat_with_actions_parameter(self, cognite_client: CogniteClient, action_call_response_body: dict) -> None:
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: action_call_response_body))

        add_action = ClientToolAction(
            name="add",
            description="Add two numbers",
            parameters={
                "type": "object",
                "properties": {
                    "a": {"type": "number"},
                    "b": {"type": "number"},
                },
                "required": ["a", "b"],
            },
        )

        response = cognite_client.agents.chat(
            agent_id="my_agent",
            messages=Message("What is 42 plus 58?"),
            actions=[add_action],
        )

        # Verify actions were sent in request
        call_args = cognite_client.agents._post.call_args
        assert "actions" in call_args[1]["json"]
        assert len(call_args[1]["json"]["actions"]) == 1
        assert call_args[1]["json"]["actions"][0]["type"] == "clientTool"
        assert call_args[1]["json"]["actions"][0]["clientTool"]["name"] == "add"

        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.action_calls is not None
        assert len(response.action_calls) == 1

    def test_chat_with_action_result_message(self, cognite_client: CogniteClient, final_response_body: dict) -> None:
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: final_response_body))

        add_action = ClientToolAction(
            name="add",
            description="Add two numbers",
            parameters={"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}}},
        )

        result = ClientToolResult(
            action_id="call_abc123",
            content="The result is 100",
        )

        response = cognite_client.agents.chat(
            agent_id="my_agent",
            messages=result,
            cursor="cursor_12345",
            actions=[add_action],
        )

        # Verify action result was sent
        call_args = cognite_client.agents._post.call_args
        assert call_args[1]["json"]["cursor"] == "cursor_12345"
        assert len(call_args[1]["json"]["messages"]) == 1
        msg = call_args[1]["json"]["messages"][0]
        assert msg["role"] == "action"
        assert msg["type"] == "clientTool"
        assert msg["actionId"] == "call_abc123"

        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.text == "The result is 100."


class TestFromFunction:
    """Test ClientToolAction.from_function() method."""

    def test_basic_function_with_primitives(self) -> None:
        """Test basic function with primitive types."""

        def add(a: float, b: float) -> float:
            """Add two numbers.

            Args:
                a: The first number.
                b: The second number.

            Returns:
                The sum.
            """
            return a + b

        action = ClientToolAction.from_function(add)

        assert action.name == "add"
        assert action.description == "Add two numbers."
        assert action.parameters == {
            "type": "object",
            "properties": {
                "a": {"type": "number", "description": "The first number."},
                "b": {"type": "number", "description": "The second number."},
            },
            "required": ["a", "b"],
        }

    def test_all_primitive_types(self) -> None:
        """Test all supported primitive types."""

        def example(
            name: str,
            count: int,
            value: float,
            enabled: bool,
        ) -> None:
            """Example function.

            Args:
                name: A string parameter.
                count: An integer parameter.
                value: A float parameter.
                enabled: A boolean parameter.
            """
            pass

        action = ClientToolAction.from_function(example)

        assert action.parameters["properties"]["name"] == {
            "type": "string",
            "description": "A string parameter.",
        }
        assert action.parameters["properties"]["count"] == {
            "type": "integer",
            "description": "An integer parameter.",
        }
        assert action.parameters["properties"]["value"] == {
            "type": "number",
            "description": "A float parameter.",
        }
        assert action.parameters["properties"]["enabled"] == {
            "type": "boolean",
            "description": "A boolean parameter.",
        }
        assert action.parameters["required"] == ["name", "count", "value", "enabled"]

    def test_list_types(self) -> None:
        """Test list parameter types."""

        def process_lists(
            names: list[str],
            scores: list[float],
            counts: list[int],
            flags: list[bool],
        ) -> None:
            """Process various lists.

            Args:
                names: List of names.
                scores: List of scores.
                counts: List of counts.
                flags: List of flags.
            """
            pass

        action = ClientToolAction.from_function(process_lists)

        assert action.parameters["properties"]["names"] == {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of names.",
        }
        assert action.parameters["properties"]["scores"] == {
            "type": "array",
            "items": {"type": "number"},
            "description": "List of scores.",
        }
        assert action.parameters["properties"]["counts"] == {
            "type": "array",
            "items": {"type": "integer"},
            "description": "List of counts.",
        }
        assert action.parameters["properties"]["flags"] == {
            "type": "array",
            "items": {"type": "boolean"},
            "description": "List of flags.",
        }

    def test_optional_parameters(self) -> None:
        """Test optional parameters (default=None)."""

        def greet(name: str, title: str | None = None) -> str:
            """Greet a person.

            Args:
                name: The person's name.
                title: Optional title.
            """
            return f"Hello, {title} {name}!" if title else f"Hello, {name}!"

        action = ClientToolAction.from_function(greet)

        assert "name" in action.parameters["required"]
        assert "title" not in action.parameters["required"]
        assert "title" in action.parameters["properties"]

    def test_custom_name(self) -> None:
        """Test custom action name override."""

        def add(a: float, b: float) -> float:
            """Add two numbers."""
            return a + b

        action = ClientToolAction.from_function(add, name="custom_add")

        assert action.name == "custom_add"

    def test_no_docstring_uses_function_name(self) -> None:
        """Test that missing docstring uses function name with warning."""

        def add(a: float, b: float) -> float:
            return a + b

        with pytest.warns(UserWarning, match="has no docstring"):
            action = ClientToolAction.from_function(add)

        assert action.name == "add"
        assert action.description == "add"

    def test_multiline_param_description(self) -> None:
        """Test parameter descriptions that span multiple lines."""

        def example(param: str) -> None:
            """Example function.

            Args:
                param: This is a long description
                    that spans multiple lines
                    in the docstring.
            """
            pass

        action = ClientToolAction.from_function(example)

        assert (
            action.parameters["properties"]["param"]["description"]
            == "This is a long description that spans multiple lines in the docstring."
        )

    def test_multiline_function_description(self) -> None:
        """Test function description that spans multiple lines."""

        def example(param: str) -> None:
            """This is a function description
            that spans multiple lines
            before the Args section.

            Args:
                param: A parameter.
            """
            pass

        action = ClientToolAction.from_function(example)

        assert action.description == "This is a function description that spans multiple lines before the Args section."

    def test_missing_type_annotation_raises_error(self) -> None:
        """Test that missing type annotation raises TypeError."""

        def bad(param) -> None:
            """Bad function."""
            pass

        with pytest.raises(TypeError, match="missing type annotation"):
            ClientToolAction.from_function(bad)

    def test_unsupported_type_raises_error(self) -> None:
        """Test that unsupported types raise TypeError."""

        def bad(data: dict) -> None:
            """Bad function.

            Args:
                data: A dict parameter.
            """
            pass

        with pytest.raises(TypeError, match="unsupported type"):
            ClientToolAction.from_function(bad)

    def test_unsupported_list_item_type_raises_error(self) -> None:
        """Test that unsupported list item types raise TypeError."""

        def bad(items: list[dict]) -> None:
            """Bad function.

            Args:
                items: A list of dicts.
            """
            pass

        with pytest.raises(TypeError, match="unsupported list item type"):
            ClientToolAction.from_function(bad)

    def test_list_without_item_type_raises_error(self) -> None:
        """Test that bare list type raises TypeError."""

        def bad(items: list) -> None:
            """Bad function.

            Args:
                items: A list.
            """
            pass

        with pytest.raises(TypeError, match="without item type"):
            ClientToolAction.from_function(bad)

    def test_function_with_no_parameters(self) -> None:
        """Test function with no parameters."""

        def get_status() -> str:
            """Get current status.

            Returns:
                The status string.
            """
            return "OK"

        action = ClientToolAction.from_function(get_status)

        assert action.name == "get_status"
        assert action.description == "Get current status."
        assert action.parameters == {
            "type": "object",
            "properties": {},
        }
        assert "required" not in action.parameters

    def test_param_without_description_in_docstring(self) -> None:
        """Test parameter without description in docstring."""

        def example(param: str) -> None:
            """Example function."""
            pass

        action = ClientToolAction.from_function(example)

        # Parameter should exist but without description
        assert "param" in action.parameters["properties"]
        assert "description" not in action.parameters["properties"]["param"]

    def test_dump_and_use_in_chat(self) -> None:
        """Test that generated action can be dumped and used in chat."""

        def add(a: float, b: float) -> float:
            """Add two numbers.

            Args:
                a: First number.
                b: Second number.
            """
            return a + b

        action = ClientToolAction.from_function(add)
        dumped = action.dump()

        assert dumped["type"] == "clientTool"
        assert dumped["clientTool"]["name"] == "add"
        assert dumped["clientTool"]["description"] == "Add two numbers."
        assert dumped["clientTool"]["parameters"]["type"] == "object"
