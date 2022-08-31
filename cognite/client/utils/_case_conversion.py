from typing import Any

from cognite.client.utils._auxiliary import to_camel_case, to_snake_case


def resource_to_camel_case(resource) -> Any:
    if isinstance(resource, list):
        return [resource_to_camel_case(element) for element in resource]
    elif isinstance(resource, dict):
        return {to_camel_case(k): resource_to_camel_case(v) for k, v in resource.items() if v is not None}
    elif hasattr(resource, "__dict__"):
        return resource_to_camel_case(resource.__dict__)
    return resource


def resource_to_snake_case(resource) -> Any:
    if isinstance(resource, list):
        return [resource_to_snake_case(element) for element in resource]
    elif isinstance(resource, dict):
        return {to_snake_case(k): resource_to_snake_case(v) for k, v in resource.items() if v is not None}
    elif hasattr(resource, "__dict__"):
        return resource_to_snake_case(resource.__dict__)
    return resource
