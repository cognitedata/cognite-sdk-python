from __future__ import annotations

from cognite.client import ClientConfig
from cognite.client._cognite_client import AsyncCogniteClient as Client


class AsyncCogniteClient(Client):
    def __init__(self, config: ClientConfig) -> None:
        config.api_subversion = "beta"
        super().__init__(config)
