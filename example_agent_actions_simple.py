"""
Example: Using Agent Actions with Cognite SDK - Simplified API

This example demonstrates the simplified actions API that uses dictionaries.
"""

import json
from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message, ClientToolAction


def main():
    # Initialize the Cognite client
    client = CogniteClient()
    
    # Example 1: Using a simple dictionary for actions
    print("Example 1: Simple dictionary action")
    print("-" * 50)
    
    # Define action as a simple dict - no complex validation
    update_status_action = {
        "type": "clientTool",
        "clientTool": {
            "name": "update_order_status",
            "description": "Update the status of an order",
            "parameters": {
                "type": "object",
                "properties": {
                    "order_id": {
                        "type": "string",
                        "description": "The order ID"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["processing", "shipped", "delivered"]
                    }
                },
                "required": ["order_id", "status"]
            }
        }
    }
    
    response = client.agents.chat(
        agent_id="my_agent",
        messages=Message("Update order ABC123 to shipped"),
        actions=[update_status_action]  # Just pass the dict!
    )
    
    # Handle the response
    if response.messages and response.messages[0].actions:
        for action in response.messages[0].actions:
            if hasattr(action, 'client_tool'):
                print(f"Agent wants to call: {action.client_tool.name}")
                print(f"With arguments: {action.client_tool.arguments}")
                
                # Parse arguments and execute
                args = json.loads(action.client_tool.arguments)
                print(f"Updating order {args['order_id']} to {args['status']}")
                
                # Send result back
                follow_up = client.agents.chat(
                    agent_id="my_agent",
                    messages=Message(
                        content=f"Order updated successfully",
                        role="action",
                        action_id=action.id
                    ),
                    cursor=response.cursor,
                    actions=[update_status_action]
                )
    
    print("\n")
    
    # Example 2: Using ClientToolAction class (optional)
    print("Example 2: Using ClientToolAction class")
    print("-" * 50)
    
    # You can still use the class if you prefer
    weather_action = ClientToolAction(
        client_tool={
            "name": "get_weather",
            "description": "Get current weather",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"}
                },
                "required": ["location"]
            }
        }
    )
    
    response = client.agents.chat(
        agent_id="my_agent",
        messages=Message("What's the weather in Oslo?"),
        actions=[weather_action]
    )
    
    print("\n")
    
    # Example 3: Multiple actions as dicts
    print("Example 3: Multiple actions")
    print("-" * 50)
    
    actions = [
        {
            "type": "clientTool",
            "clientTool": {
                "name": "calculate",
                "description": "Perform calculations",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "expression": {"type": "string"}
                    }
                }
            }
        },
        {
            "type": "clientTool", 
            "clientTool": {
                "name": "search_database",
                "description": "Search internal database",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "limit": {"type": "integer", "default": 10}
                    }
                }
            }
        }
    ]
    
    response = client.agents.chat(
        agent_id="my_agent",
        messages=Message("Calculate 15% of 200 and search for recent orders"),
        actions=actions  # Just pass the list of dicts!
    )
    
    # Process multiple action requests
    if response.messages:
        for message in response.messages:
            if message.actions:
                results = []
                for action in message.actions:
                    if hasattr(action, 'client_tool'):
                        args = json.loads(action.client_tool.arguments)
                        
                        if action.client_tool.name == "calculate":
                            result = f"Result: {eval(args['expression'])}"  # In real code, use safe evaluation
                        elif action.client_tool.name == "search_database":
                            result = f"Found {args.get('limit', 10)} orders matching '{args['query']}'"
                        else:
                            result = "Unknown action"
                        
                        results.append(Message(
                            content=result,
                            role="action",
                            action_id=action.id
                        ))
                
                # Send all results back
                if results:
                    follow_up = client.agents.chat(
                        agent_id="my_agent",
                        messages=results,
                        cursor=response.cursor,
                        actions=actions
                    )
                    print(f"Final response: {follow_up.text}")


if __name__ == "__main__":
    main()