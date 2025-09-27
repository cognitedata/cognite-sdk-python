"""Tests for client tool data classes."""

from __future__ import annotations

from cognite.client.data_classes.agents.client_tools import ClientTool, ClientToolList, ClientToolParameters


class TestClientToolParameters:
    def test_init_defaults(self) -> None:
        """Test ClientToolParameters initialization with defaults."""
        params = ClientToolParameters()
        assert params.type == "object"
        assert params.properties == {}
        assert params.required == []
        assert params.property_ordering == []
        assert params.description is None

    def test_init_with_values(self) -> None:
        """Test ClientToolParameters initialization with values."""
        properties = {"location": {"type": "string", "description": "City name"}}
        required = ["location"]
        property_ordering = ["location"]
        description = "Parameters for weather tool"

        params = ClientToolParameters(
            type="object",
            properties=properties,
            required=required,
            property_ordering=property_ordering,
            description=description,
        )

        assert params.type == "object"
        assert params.properties == properties
        assert params.required == required
        assert params.property_ordering == property_ordering
        assert params.description == description

    def test_dump_camel_case(self) -> None:
        """Test dumping ClientToolParameters with camel case."""
        params = ClientToolParameters(
            properties={"location": {"type": "string"}},
            required=["location"],
            property_ordering=["location"],
            description="Test parameters",
        )

        result = params.dump(camel_case=True)
        expected = {
            "type": "object",
            "properties": {"location": {"type": "string"}},
            "required": ["location"],
            "propertyOrdering": ["location"],
            "description": "Test parameters",
        }
        assert result == expected

    def test_dump_snake_case(self) -> None:
        """Test dumping ClientToolParameters with snake case."""
        params = ClientToolParameters(
            properties={"location": {"type": "string"}},
            required=["location"],
            property_ordering=["location"],
            description="Test parameters",
        )

        result = params.dump(camel_case=False)
        expected = {
            "type": "object",
            "properties": {"location": {"type": "string"}},
            "required": ["location"],
            "property_ordering": ["location"],
            "description": "Test parameters",
        }
        assert result == expected

    def test_load_from_dict(self) -> None:
        """Test loading ClientToolParameters from dictionary."""
        data = {
            "type": "object",
            "properties": {"location": {"type": "string"}},
            "required": ["location"],
            "propertyOrdering": ["location"],
            "description": "Test parameters",
        }

        params = ClientToolParameters._load(data)
        assert params.type == "object"
        assert params.properties == {"location": {"type": "string"}}
        assert params.required == ["location"]
        assert params.property_ordering == ["location"]
        assert params.description == "Test parameters"

    def test_load_from_dict_snake_case(self) -> None:
        """Test loading ClientToolParameters from dictionary with snake case."""
        data = {
            "type": "object",
            "properties": {"location": {"type": "string"}},
            "required": ["location"],
            "property_ordering": ["location"],
            "description": "Test parameters",
        }

        params = ClientToolParameters._load(data)
        assert params.type == "object"
        assert params.properties == {"location": {"type": "string"}}
        assert params.required == ["location"]
        assert params.property_ordering == ["location"]
        assert params.description == "Test parameters"


class TestClientTool:
    def test_init_defaults(self) -> None:
        """Test ClientTool initialization with defaults."""
        parameters = ClientToolParameters()
        tool = ClientTool(name="test_tool", parameters=parameters)
        assert tool.name == "test_tool"
        assert tool.parameters is parameters
        assert tool.type == "clientTool"
        assert tool.description is None

    def test_init_with_values(self) -> None:
        """Test ClientTool initialization with values."""
        parameters = ClientToolParameters(properties={"location": {"type": "string"}})
        description = "A test tool"
        tool_type = "clientTool"

        tool = ClientTool(
            name="test_tool",
            parameters=parameters,
            description=description,
            type=tool_type,
        )

        assert tool.name == "test_tool"
        assert tool.parameters is parameters
        assert tool.type == tool_type
        assert tool.description == description

    def test_dump_camel_case(self) -> None:
        """Test dumping ClientTool with camel case."""
        parameters = ClientToolParameters(
            properties={"location": {"type": "string"}},
            required=["location"],
        )
        tool = ClientTool(
            name="test_tool",
            parameters=parameters,
            description="A test tool",
        )

        result = tool.dump(camel_case=True)
        expected = {
            "name": "test_tool",
            "type": "clientTool",
            "description": "A test tool",
            "parameters": {
                "type": "object",
                "properties": {"location": {"type": "string"}},
                "required": ["location"],
                "propertyOrdering": [],
            },
        }
        assert result == expected

    def test_dump_without_description(self) -> None:
        """Test dumping ClientTool without description."""
        parameters = ClientToolParameters()
        tool = ClientTool(name="test_tool", parameters=parameters)

        result = tool.dump(camel_case=True)
        expected = {
            "name": "test_tool",
            "type": "clientTool",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
                "propertyOrdering": [],
            },
        }
        assert result == expected

    def test_load_from_dict(self) -> None:
        """Test loading ClientTool from dictionary."""
        data = {
            "name": "test_tool",
            "type": "clientTool",
            "description": "A test tool",
            "parameters": {
                "type": "object",
                "properties": {"location": {"type": "string"}},
                "required": ["location"],
                "propertyOrdering": [],
            },
        }

        tool = ClientTool._load(data)
        assert tool.name == "test_tool"
        assert tool.type == "clientTool"
        assert tool.description == "A test tool"
        assert isinstance(tool.parameters, ClientToolParameters)
        assert tool.parameters.properties == {"location": {"type": "string"}}
        assert tool.parameters.required == ["location"]


class TestClientToolList:
    def test_init(self) -> None:
        """Test ClientToolList initialization."""
        tool1 = ClientTool(name="tool1", parameters=ClientToolParameters())
        tool2 = ClientTool(name="tool2", parameters=ClientToolParameters())
        tools = [tool1, tool2]

        tool_list = ClientToolList(tools)
        assert list(tool_list) == tools

    def test_dump_camel_case(self) -> None:
        """Test dumping ClientToolList with camel case."""
        tool1 = ClientTool(
            name="tool1",
            parameters=ClientToolParameters(properties={"param1": {"type": "string"}}),
        )
        tool2 = ClientTool(
            name="tool2",
            parameters=ClientToolParameters(properties={"param2": {"type": "integer"}}),
        )
        tool_list = ClientToolList([tool1, tool2])

        result = tool_list.dump(camel_case=True)
        assert len(result) == 2
        assert result[0]["name"] == "tool1"
        assert result[1]["name"] == "tool2"
        assert result[0]["parameters"]["properties"]["param1"]["type"] == "string"
        assert result[1]["parameters"]["properties"]["param2"]["type"] == "integer"

    def test_load_from_list(self) -> None:
        """Test loading ClientToolList from list of dictionaries."""
        data = [
            {
                "name": "tool1",
                "type": "clientTool",
                "parameters": {
                    "type": "object",
                    "properties": {"param1": {"type": "string"}},
                    "required": [],
                    "propertyOrdering": [],
                },
            },
            {
                "name": "tool2",
                "type": "clientTool",
                "parameters": {
                    "type": "object",
                    "properties": {"param2": {"type": "integer"}},
                    "required": [],
                    "propertyOrdering": [],
                },
            },
        ]

        tool_list = ClientToolList._load(data)
        assert len(tool_list) == 2
        assert tool_list[0].name == "tool1"
        assert tool_list[1].name == "tool2"
        assert isinstance(tool_list[0], ClientTool)
        assert isinstance(tool_list[1], ClientTool)
