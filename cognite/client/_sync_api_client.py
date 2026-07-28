from abc import ABC


class SyncAPIClient(ABC):
    """Base class for all synchronous API clients. No real use besides easy isinstance checks in e.g. testing."""

    pass
