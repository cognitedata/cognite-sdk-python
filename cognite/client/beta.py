from typing import Any

from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["api_subversion"] = "beta"
        super().__init__(*args, **kwargs)
