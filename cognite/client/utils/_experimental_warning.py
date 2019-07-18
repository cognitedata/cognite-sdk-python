import functools
import warnings

from cognite.client.exceptions import CogniteExperimentalFeature
from cognite.client.utils._client_config import _DefaultConfig

WARNED_EXPERIMENTAL_APIS = set()


def experimental_fn(func=None, api_name=None):
    if func is None:
        return functools.partial(experimental_fn, api_name=api_name)

    experimental_warning_message = (
        "\nThe {} API is currently experimental, so this functionality does not adhere to semantic versionining."
        "\nThis means that this API may be subject to breaking changes even between patch versions."
        "\nYou should NOT use the {} API in any production code.".format(api_name, api_name)
    )

    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if not _DefaultConfig().enable_experimental:
            raise CogniteExperimentalFeature(
                experimental_warning_message + "\nIf you still want to use this feature, set the environment variable "
                "COGNITE_EXPERIMENTAL_MODE = '1' to enable this functionality."
            )
        elif api_name not in WARNED_EXPERIMENTAL_APIS:
            warnings.warn(experimental_warning_message, stacklevel=2, category=FutureWarning)
            WARNED_EXPERIMENTAL_APIS.add(api_name)
        return func(*args, **kwargs)

    return decorator


def experimental_api(cls=None, api_name=None):
    """A class decorator that will raise warnings once an experimental API is used."""
    if cls is None:
        return functools.partial(experimental_api, api_name=api_name)
    for attr in cls.__dict__:
        if not attr.startswith("_") and callable(getattr(cls, attr)):
            setattr(cls, attr, experimental_fn(api_name=api_name)(getattr(cls, attr)))
    return cls
