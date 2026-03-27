from collections.abc import Callable


def create_handler() -> Callable[[dict, object, dict, object, object], dict]:
    def inner_function(data: dict, client: object, secrets: dict, invalid_arg: object, another_invalid: object) -> dict:
        return {"assetId": 1234}

    return inner_function


handle = create_handler()
