from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Agent, AgentSession, Message
from cognite.client.data_classes.agents.chat import (
    AgentChatResponse,
    AgentMessage,
    TextContent,
)
from cognite.client.exceptions import CogniteMissingClientError


@pytest.fixture
def chat_response_body() -> dict:
    return {
        "agentId": "my_agent",
        "response": {
            "cursor": "cursor_12345",
            "messages": [
                {
                    "content": {
                        "text": "Hello! How can I help you today?",
                        "type": "text",
                    },
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


@pytest.fixture
def followup_response_body() -> dict:
    return {
        "agentId": "my_agent",
        "response": {
            "cursor": "cursor_67890",
            "messages": [
                {
                    "content": {
                        "text": "I can help you with data analysis, finding assets, and more!",
                        "type": "text",
                    },
                    "role": "agent",
                }
            ],
            "type": "result",
        },
    }


class TestAgentSession:
    def test_start_session_from_agents_api(self, cognite_client: CogniteClient) -> None:
        """Test starting a session from the agents API."""
        session = cognite_client.agents.start_session("my_agent")
        
        assert isinstance(session, AgentSession)
        assert session.agent_id == "my_agent"
        assert session.cursor is None
        assert session._cognite_client is cognite_client

    def test_start_session_with_cursor(self, cognite_client: CogniteClient) -> None:
        """Test starting a session with an existing cursor."""
        initial_cursor = "existing_cursor_123"
        session = cognite_client.agents.start_session("my_agent", cursor=initial_cursor)
        
        assert session.agent_id == "my_agent"
        assert session.cursor == initial_cursor

    def test_session_chat_with_message(self, cognite_client: CogniteClient, chat_response_body: dict) -> None:
        """Test chatting with a Message object."""
        # Mock the API response
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: chat_response_body))
        
        session = cognite_client.agents.start_session("my_agent")
        response = session.chat(Message("Hello"))
        
        # Verify the underlying chat method was called correctly
        cognite_client.agents._post.assert_called_once()
        call_args = cognite_client.agents._post.call_args
        assert call_args[1]["json"]["agentId"] == "my_agent"
        assert call_args[1]["json"]["messages"][0]["content"]["text"] == "Hello"
        assert call_args[1]["json"]["messages"][0]["role"] == "user"
        assert "cursor" not in call_args[1]["json"]  # No cursor on first message
        
        # Verify response
        assert isinstance(response, AgentChatResponse)
        assert response.text == "Hello! How can I help you today?"
        assert session.cursor == "cursor_12345"  # Cursor should be updated



    def test_session_cursor_management(
        self, 
        cognite_client: CogniteClient, 
        chat_response_body: dict, 
        followup_response_body: dict
    ) -> None:
        """Test that cursor is automatically managed across multiple interactions."""
        # Set up mock responses for two consecutive calls
        responses = [
            MagicMock(json=lambda: chat_response_body),
            MagicMock(json=lambda: followup_response_body),
        ]
        cognite_client.agents._post = MagicMock(side_effect=responses)
        
        session = cognite_client.agents.start_session("my_agent")
        
        # First interaction
        response1 = session.chat(Message("Hello"))
        assert session.cursor == "cursor_12345"
        
        # Second interaction - should include cursor from first response
        response2 = session.chat(Message("Tell me more"))
        assert session.cursor == "cursor_67890"  # Updated to new cursor
        
        # Verify the second call included the cursor from the first response
        second_call_args = cognite_client.agents._post.call_args_list[1]
        assert second_call_args[1]["json"]["cursor"] == "cursor_12345"
        assert second_call_args[1]["json"]["messages"][0]["content"]["text"] == "Tell me more"

    def test_session_chat_with_multiple_messages(self, cognite_client: CogniteClient, chat_response_body: dict) -> None:
        """Test chatting with multiple messages at once."""
        cognite_client.agents._post = MagicMock(return_value=MagicMock(json=lambda: chat_response_body))
        
        session = cognite_client.agents.start_session("my_agent")
        messages = [
            Message("Find temperature sensors"),
            Message("Show me their recent data")
        ]
        response = session.chat(messages)
        
        # Verify multiple messages were sent
        call_args = cognite_client.agents._post.call_args
        assert len(call_args[1]["json"]["messages"]) == 2
        assert call_args[1]["json"]["messages"][0]["content"]["text"] == "Find temperature sensors"
        assert call_args[1]["json"]["messages"][1]["content"]["text"] == "Show me their recent data"


class TestAgentStartSession:
    def test_agent_start_session(self, cognite_client: CogniteClient) -> None:
        """Test starting a session from an Agent instance."""
        # Create an agent instance with a cognite client
        agent = Agent(
            external_id="my_agent",
            name="My Agent",
            description="Test agent"
        )
        agent._cognite_client = cognite_client
        
        session = agent.start_session()
        
        assert isinstance(session, AgentSession)
        assert session.agent_id == "my_agent"
        assert session.cursor is None
        assert session._cognite_client is cognite_client

    def test_agent_start_session_with_cursor(self, cognite_client: CogniteClient) -> None:
        """Test starting a session from an Agent instance with a cursor."""
        agent = Agent(
            external_id="my_agent",
            name="My Agent"
        )
        agent._cognite_client = cognite_client
        
        initial_cursor = "existing_cursor_456"
        session = agent.start_session(cursor=initial_cursor)
        
        assert session.agent_id == "my_agent"
        assert session.cursor == initial_cursor

    def test_agent_start_session_without_client_raises_error(self) -> None:
        """Test that starting a session without a cognite client raises an error."""
        agent = Agent(
            external_id="my_agent",
            name="My Agent"
        )
        # Don't set _cognite_client
        
        with pytest.raises(CogniteMissingClientError):
            agent.start_session()

    def test_agent_start_session_with_none_client_raises_error(self) -> None:
        """Test that starting a session with None cognite client raises an error."""
        agent = Agent(
            external_id="my_agent",
            name="My Agent"
        )
        agent._cognite_client = None
        
        with pytest.raises(CogniteMissingClientError):
            agent.start_session()


class TestIntegrationWorkflow:
    def test_complete_workflow(
        self, 
        cognite_client: CogniteClient, 
        chat_response_body: dict, 
        followup_response_body: dict
    ) -> None:
        """Test the complete workflow as described in the user requirements."""
        # Set up mock responses
        responses = [
            MagicMock(json=lambda: chat_response_body),
            MagicMock(json=lambda: followup_response_body),
        ]
        cognite_client.agents._post = MagicMock(side_effect=responses)
        
        # Mock the retrieve method to return an agent with cognite_client
        mock_agent = Agent(external_id="my-agent", name="Test Agent")
        mock_agent._cognite_client = cognite_client
        cognite_client.agents.retrieve = MagicMock(return_value=mock_agent)
        
        # Test the workflow from the user requirements
        
        # 1. Start session from agents API
        session = cognite_client.agents.start_session(agent_id="my-agent", cursor=None)
        
        # 2. First chat
        resp = session.chat(Message("hello"))
        assert resp.text == "Hello! How can I help you today?"
        
        # 3. Follow-up question with automatic cursor management
        followup_response = session.chat(Message("nice to meet you"))
        assert followup_response.text == "I can help you with data analysis, finding assets, and more!"
        
        # 4. Test starting session from agent instance
        agent = cognite_client.agents.retrieve("my-agent")
        agent_session = agent.start_session()  # also with optional cursor
        
        assert isinstance(agent_session, AgentSession)
        assert agent_session.agent_id == "my-agent"
        
        # Verify that cursors were managed correctly
        first_call_args = cognite_client.agents._post.call_args_list[0]
        second_call_args = cognite_client.agents._post.call_args_list[1]
        
        # First call should have no cursor
        assert "cursor" not in first_call_args[1]["json"]
        
        # Second call should have cursor from first response
        assert second_call_args[1]["json"]["cursor"] == "cursor_12345"