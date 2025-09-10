#!/usr/bin/env python3
"""
Example demonstrating how to use actions with the Cognite Agents Chat API.

This example shows:
1. How to define client tools (actions)
2. How to send actions to an agent
3. How to handle action responses from the agent
4. How to create a complete action workflow
"""

import json
from typing import Any, Dict, List

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import (
    Action,
    ActionMessage,
    ClientTool,
    Message,
    TextContent,
)


def calculate_average(numbers: List[float]) -> float:
    """Example function that could be called by an agent action."""
    return sum(numbers) / len(numbers)


def get_weather_info(location: str) -> Dict[str, Any]:
    """Example function to get weather information."""
    # In a real implementation, this would call a weather API
    return {
        "location": location,
        "temperature": 22.5,
        "humidity": 65,
        "condition": "partly cloudy",
    }


def main():
    """Main example function demonstrating actions usage."""
    client = CogniteClient()

    # Example 1: Define client tools (actions)
    print("=== Example 1: Defining Client Tools ===")
    
    # Define a tool for calculating averages
    calculate_tool = ClientTool(
        name="calculate_average",
        description="Calculate the average of a list of numbers",
        parameters={
            "type": "object",
            "properties": {
                "numbers": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "List of numbers to average"
                }
            },
            "required": ["numbers"]
        }
    )
    
    # Define a tool for weather information
    weather_tool = ClientTool(
        name="get_weather",
        description="Get current weather information for a location",
        parameters={
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "The city or location to get weather for"
                }
            },
            "required": ["location"]
        }
    )
    
    # Create actions from the tools
    calculate_action = Action(type="clientTool", client_tool=calculate_tool)
    weather_action = Action(type="clientTool", client_tool=weather_tool)
    
    print(f"Created calculate action: {calculate_action.client_tool.name}")
    print(f"Created weather action: {weather_action.client_tool.name}")
    print()

    # Example 2: Chat with actions
    print("=== Example 2: Chatting with Actions ===")
    
    try:
        # Send a message with actions available
        response = client.agents.chat(
            agent_id="my_agent",
            messages=Message("I need to calculate the average of 10, 20, 30, 40, 50. Can you help?"),
            actions=[calculate_action, weather_action]
        )
        
        print(f"Agent response: {response.text}")
        
        # Check if the agent wants to use any actions
        if response.messages and response.messages[0].actions:
            print("\n=== Example 3: Handling Agent Actions ===")
            for action in response.messages[0].actions:
                if action.type == "clientTool":
                    tool_name = action.client_tool.get("name")
                    tool_args = action.client_tool.get("arguments", "{}")
                    
                    print(f"Agent wants to call: {tool_name}")
                    print(f"With arguments: {tool_args}")
                    
                    # Parse arguments and execute the function
                    try:
                        args_dict = json.loads(tool_args) if isinstance(tool_args, str) else tool_args
                        
                        if tool_name == "calculate_average":
                            result = calculate_average(args_dict["numbers"])
                            result_message = f"The average is: {result}"
                        elif tool_name == "get_weather":
                            result = get_weather_info(args_dict["location"])
                            result_message = f"Weather info: {json.dumps(result, indent=2)}"
                        else:
                            result_message = f"Unknown tool: {tool_name}"
                        
                        print(f"Function result: {result_message}")
                        
                        # Send the result back to the agent
                        follow_up_response = client.agents.chat(
                            agent_id="my_agent",
                            messages=ActionMessage(
                                action_id=action.id,
                                content=TextContent(text=result_message)
                            ),
                            cursor=response.cursor
                        )
                        
                        print(f"Agent follow-up: {follow_up_response.text}")
                        
                    except Exception as e:
                        print(f"Error executing function: {e}")
                        
                        # Send error back to agent
                        error_response = client.agents.chat(
                            agent_id="my_agent",
                            messages=ActionMessage(
                                action_id=action.id,
                                content=TextContent(text=f"Error: {str(e)}")
                            ),
                            cursor=response.cursor
                        )
                        print(f"Agent error handling: {error_response.text}")
        
        print()
        
    except Exception as e:
        print(f"Error in chat: {e}")

    # Example 4: Complex action workflow
    print("=== Example 4: Complex Action Workflow ===")
    
    # Define a more complex tool
    data_analysis_tool = ClientTool(
        name="analyze_data",
        description="Analyze a dataset and return insights",
        parameters={
            "type": "object",
            "properties": {
                "data": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Array of numerical data points"
                },
                "analysis_type": {
                    "type": "string",
                    "enum": ["mean", "median", "std_dev", "min_max"],
                    "description": "Type of analysis to perform"
                }
            },
            "required": ["data", "analysis_type"]
        }
    )
    
    analysis_action = Action(type="clientTool", client_tool=data_analysis_tool)
    
    try:
        # Start a conversation about data analysis
        response = client.agents.chat(
            agent_id="my_agent",
            messages=Message("I have this dataset: [1, 5, 3, 8, 2, 9, 4, 7, 6]. Please analyze it for me."),
            actions=[analysis_action]
        )
        
        print(f"Agent response: {response.text}")
        
        # This would continue the workflow based on agent actions...
        
    except Exception as e:
        print(f"Error in complex workflow: {e}")

    # Example 5: Action with mixed message types
    print("\n=== Example 5: Mixed Message Types ===")
    
    try:
        # Send both regular messages and action messages
        mixed_messages = [
            Message("Let me provide some context first."),
            ActionMessage(
                action_id="previous_calculation",
                content=TextContent(text="Previous calculation result: 25.5")
            ),
            Message("Now please help me with a new calculation.")
        ]
        
        response = client.agents.chat(
            agent_id="my_agent",
            messages=mixed_messages,
            actions=[calculate_action]
        )
        
        print(f"Agent response to mixed messages: {response.text}")
        
    except Exception as e:
        print(f"Error with mixed messages: {e}")


if __name__ == "__main__":
    main()