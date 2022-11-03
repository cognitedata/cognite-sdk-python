# Migration Guide
Changes are grouped as follows:
- `Dependency` for new hard requirements that you must make sure work with your project before upgrading
- `Deprecated` for methods/features that are still available, but will be removed in a future release
- `Removed` for methods/features that have been removed (and what to do now!)
- `Function Signature` for methods/functions with a change to its function signature, e.g. change in default values
- `Functionality` for changes that are not backwards compatible
- `Changed` for changes that do not fall into any other category
- `Optional` for new, optional methods/features that you should be aware of - *and could take advantage of*

## From v4 to v5

### Dependency
- Required dependency, `protobuf`.

### Deprecated
- Consider changing `client.datapoints` (deprecated) to `client.time_series.data`.

### Removed
- `DatapointsAPI.query`: All three retrieve-endpoints now support arbitrarily complex queries. See docstring of `DatapointsAPI.retrieve` for example usage.
- `DatapointsAPI.retrieve_dataframe_dict`: Not needed as the combination of `.get` and e.g. `.average` is covers the same use case - with simpler syntax.
- Exception `CogniteDuplicateColumnsError`: Duplicated identifiers in datapoint queries are now allowed.
- All convenience methods related to plotting and the use of `matplotlib` (go ask your FNDS <3 (Friendly Neighborhood Data Scientist).

### Function Signature
- All Cognite resources types have a `to_pandas` (or `to_geopandas`) method. Previously, these had various defaults for the `camel_case` parameter, but they have all been changed to `False`.
- `DatapointsAPI.insert_dataframe` has a new default values for `dropna`; changed from `False` to `True` (functionality unchanged, applies per-column)) and `external_id_headers`, also changed from `False` to `True` to disincentivize the use of internal IDs.
- `DatapointsAPI.retrieve_dataframe` no longer has a mix of `id`s and `external_id`s in the columns when given identifiers for both arguments. Instead, the user might select either by passing the new parameter `column_names`.
- `DatapointsAPI.retrieve_dataframe` no longer support the `complete` parameter.
- `DatapointsAPI.retrieve_dataframe` now accepts the parameter `uniform_index (bool)`. See docstring for explanation and usage examples.

### Functionality
- Fetching raw datapoints with a finite `limit` and `include_outside_points=True` now returns both outside points (if they exist) regardless of the given `limit`, meaning the returned datapoints count is up to `limit+2`.
- Fetching aggregate datapoints now include the time period(s) (given by the `granularity` unit) that `start` and `end` are part of (used to be only the "fully in-between" points).
- The `get` method of `DatapointsList` and `DatapointsArrayList` now return a list of `Datapoints` (or list of `DatapointsArray`) for all identifiers that were duplicated in the user's query.
- The utility function `datetime_to_ms` no longer issues a `FutureWarning` on missing timezone information. It will interpret naive `datetime`s as local time as is Python's default interpretation.
- The utility function `ms_to_datetime` no longer issues a `FutureWarning` on returning a naive `datetime` in UTC. It will now return an aware `datetime` object in UTC.
- The utility function `ms_to_datetime` no longer raises `ValueError` for inputs from before 1970, but will raise for input outside the allowed minimum- and maximum supported timestamps in the API.


### Optional
- Resource types `DatapointsArray` and `DatapointsArrayList` which offer way more efficient memory storage for datapoints (uses `numpy.ndarrays`)
- Datapoints fetching method, `DatapointsAPI.retrieve_arrays`, returning `DatapointsArray` and `DatapointsArrayList`.
- A single aggregate, e.g. `average`, can be specified by a string directly, `aggregates="average"` (as opposed to packing it inside a list, `aggregates=["average"]`)


## From v3 to v4
...

## From v2 to v3
...

## From v1 to v2
...
