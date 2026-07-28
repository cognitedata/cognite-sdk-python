from pathlib import Path

FOUR_SPACES = " " * 4
EIGHT_SPACES = " " * 8
KNOWN_FILES_SKIP_LIST = {
    Path("cognite/client/_api/datapoint_tasks.py"),
    Path("cognite/client/_api/functions/utils.py"),
}
MAYBE_IMPORTS = (
    "SortSpec: TypeAlias",
    "_FILTERS_SUPPORTED: frozenset[type[Filter]]",
    "AggregateAssetProperty: TypeAlias",
    "Source: TypeAlias",
    "RunStatus: TypeAlias",
    "WorkflowIdentifier: TypeAlias",
    "WorkflowVersionIdentifier: TypeAlias",
    "ComparableCapability: TypeAlias",
)
ASYNC_API_DIR = Path("cognite/client/_api")
SYNC_API_DIR = Path("cognite/client/_sync_api")
SYNC_CLIENT_PATH = Path("cognite/client/_sync_cognite_client.py")


# Why is dunder call in both "to keep" lists?! Story time:
# There's more to it than just an isinstance check: 'async def __call__' does not return
# a coroutine, but an async generator. This in turn means that mypy forces the overloads
# to NOT be 'async def' but just 'def'. Wait what?! I for sure had to Google it. So we need
# to treat e.g. __call__ as a special case in order to not lose all that typing goodies...
SYNC_METHODS_TO_KEEP = {
    "compare_capabilities",
    "__call__",
}
ASYNC_METHODS_TO_KEEP = {
    "_unsafely_wipe_and_regenerate_dml",
    "__call__",
}
