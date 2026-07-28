from collections.abc import Callable


def create_handler() -> Callable[[dict, object, dict], dict]:
    def inner_function(data: dict, client: object, secrets: dict) -> dict:
        return {"assetId": 1234}

    return inner_function


handle: Callable = create_handler()
