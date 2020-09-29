import functools
import warnings

WARNED_EXPERIMENTAL_APIS = set()


def experimental_fn(func=None, api_name=None):
    if func is None:
        return functools.partial(experimental_fn, api_name=api_name)

    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if api_name not in WARNED_EXPERIMENTAL_APIS:
            warnings.warn(
                "\nThe {} API is currently experimental, so this functionality does not adhere to semantic versionining."
                "\nThis means that this API may be subject to breaking changes even between patch versions."
                "\nThe experimental client is deprecated and will be removed soon. Consider using the 'beta' client instead.".format(
                    api_name, api_name
                ),
                stacklevel=2,
                category=FutureWarning,
            )
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
