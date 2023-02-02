# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The changelog for SDK version 0.x.x can be found [here](https://github.com/cognitedata/cognite-sdk-python/blob/0.13/CHANGELOG.md).

For users wanting to upgrade major version, a migration guide can be found [here](MIGRATION_GUIDE.md).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Improved` for transparent changes, e.g. better performance.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [5.4.0] - 02-02-23
### Added
- Support for aggregating metadata keys/values for assets

## [5.3.7] - 01-02-23
### Improved
- Issues with the SessionsAPI documentation have been addressed, and the `.create()` have been further clarified.

## [5.3.6] - 30-01-23
### Changed
- A file-not-found error has been changed from `TypeError` to `FileNotFoundError` as part of the validation in FunctionsAPI.

## [5.3.5] - 25-01-23
### Fixed
- Fixed an atexit-exception (`TypeError: '<' not supported between instances of 'tuple' and 'NoneType'`) that could be raised on PY39+ after fetching datapoints (which uses a custom thread pool implementation).

## [5.3.4] - 25-01-23
### Fixed
- Displaying Cognite resources like an `Asset` or a `TimeSeriesList` in a Jupyter notebook or similar environments depending on `._repr_html_`, no longer raises `CogniteImportError` stating that `pandas` is required. Instead, a warning is issued and `.dump()` is used as fallback.

## [5.3.3] - 24-01-23
### Added
- New parameter `token_cache_path` now accepted by `OAuthInteractive` and `OAuthDeviceCode` to allow overriding location of token cache.

### Fixed
- Platform dependent temp directory for the caching of the token in `OAuthInteractive` and `OAuthDeviceCode` (no longer crashes at exit on Windows).

## [5.3.2] - 24-01-23
### Changed
- Update pytest and other dependencies to get rid of dependency on the `py` package.

## [5.3.1] - 20-01-23
### Fixed
- Last possible valid timestamp would not be returned as first (if first by some miracle...) by the `TimeSeries.first` method due to `end` being exclusive.

## [5.3.0] - 20-01-23
### Added
- `DatapointsAPI.retrieve_latest` now support customising the `before` argument, by passing one or more objects of the newly added `LatestDatapointQuery` class.

## [5.2.0] - 19-01-23
### Changed
- The SDK has been refactored to support `protobuf>=3.16.0` (no longer requires v4 or higher). This was done to fix dependency conflicts with several popular Python packages like `tensorflow` and `streamlit` - and also Azure Functions - that required major version 3.x of `protobuf`.

## [5.1.1] - 19-01-23
### Changed
- Change RAW rows insert chunk size to make individual requests faster.

## [5.1.0] - 03-01-23
### Added
- The diagram detect function can take file reference objects that contain file (external) id as well as a page range. This is an alternative to the lists of file ids or file external ids that are still possible to use. Page ranges were not possible to specify before.

## [5.0.2] - 15-12-22
### Changed
- The valid time range for datapoints has been increased to support timestamps up to end of the year 2099 in the TimeSeriesAPI. The utility function `ms_to_datetime` has been updated accordingly.

## [5.0.1] - 07-12-22
### Fixed
- `DatapointsArray.dump` would return timestamps in nanoseconds instead of milliseconds when `convert_timestamps=False`.
- Converting a `Datapoints` object coming from a synthetic datapoints query to a `pandas.DataFrame` would, when passed `include_errors=True`, starting in version `5.0.0`, erroneously cast the `error` column to a numeric data type and sort it *before* the returned values. Both of these behaviours have been reverted.
- Several documentation issues: Missing methods, wrong descriptions through inheritance and some pure visual/aesthetic.

## [5.0.0] - 06-12-22
### Improved
- Greatly increased speed of datapoints fetching (new adaptable implementation and change from `JSON` to `protobuf`), especially when asking for... (measured in fetched `dps/sec` using the new `retrieve_arrays` method, with default settings for concurrency):
  - A large number of time series
    - 200 ts: ~1-4x speedup
    - 8000 ts: ~4-7x speedup
    - 20k-100k ts: Up to 20x faster
  - Very few time series (1-3)
    - Up to 4x faster
  - Very dense time series (>>10k dps/day)
    - Up to 5x faster
  - Any query for `string` datapoints
    - Faster the more dps, e.g. single ts, 500k: 6x speedup
- Peak memory consumption (for numeric data) is 0-55 % lower when using `retrieve` and 65-75 % lower for the new `retrieve_arrays` method.
- Fetching newly inserted datapoints no longer suffers from (potentially) very long wait times (or timeout risk).
- Converting fetched datapoints to a Pandas `DataFrame` via `to_pandas()` has changed from `O(N)` to `O(1)`, i.e., speedup no longer depends on the number of datapoints and is typically 4-5 orders of magnitude faster (!). NB: Only applies to `DatapointsArray` as returned by the `retrieve_arrays` method.
- Full customizability of queries is now available for *all retrieve* endpoints, thus the `query()` is no longer needed and has been removed. Previously only `aggregates` could be individually specified. Now all parameters can be passed either as top-level or as *individual settings*, even `ignore_unknown_ids`. This is now aligned with the API (except `ignore_unknown_ids` making the SDK arguably better!).
- Documentation for the retrieve endpoints has been overhauled with lots of new usage patterns and better examples. **Check it out**!
- Vastly better test coverage for datapoints fetching logic. You may have increased trust in the results from the SDK!

### Added
- New required dependency, `protobuf`. This is currently only used by the DatapointsAPI, but other endpoints may be changed without needing to release a new major version.
- New optional dependency, `numpy`.
- A new datapoints fetching method, `retrieve_arrays`, that loads data directly into NumPy arrays for improved speed and *much* lower memory usage.
- These arrays are stored in the new resource types `DatapointsArray` with corresponding container (list) type, `DatapointsArrayList` which offer much more efficient memory usage. `DatapointsArray` also offer zero-overhead pandas-conversion.
- `DatapointsAPI.insert` now also accepts `DatapointsArray`. It also does basic error checking like making sure the number of datapoints match the number of timestamps, and that it contains raw datapoints (as opposed to aggregate data which raises an error). This also applies to `Datapoints` input.
- `DatapointsAPI.insert_multiple` now accepts `Datapoints` and `DatapointsArray` as part of the (possibly) multiple inputs. Applies the same error checking as `insert`.

### Changed
- Datapoints are no longer fetched using `JSON`: the age of `protobuf` has begun.
- The main way to interact with the `DatapointsAPI` has been moved from `client.datapoints` to `client.time_series.data` to align and unify with the `SequenceAPI`. All example code has been updated to reflect this change. Note, however, that the `client.datapoints` will still work until the next major release, but will until then issue a `DeprecationWarning`.
- All parameters to all retrieve methods are now keyword-only (meaning no positional arguments are supported).
- All retrieve methods now accept a string for the `aggregates` parameter when asking for just one, e.g. `aggregates="max"`. This short-cut avoids having to wrap it inside a list. Both `snake_case` and `camelCase` are supported.
- The utility function `datetime_to_ms` no longer issues a `FutureWarning` on missing timezone information. It will now interpret naive `datetime`s as local time as is Python's default interpretation.
- The utility function `ms_to_datetime` no longer issues a `FutureWarning` on returning a naive `datetime` in UTC. It will now return an aware `datetime` object in UTC.
- All data classes in the SDK that represent a Cognite resource type have a `to_pandas` (or `to_geopandas`) method. Previously, these had various defaults for the `camel_case` parameter, but they have all been changed to `False`.
- All retrieve methods (when passing dict(s) with query settings) now accept identifier and aggregates in snake case (and camel case for convenience / backwards compatibility). Note that all newly added/supported customisable parameters (e.g. `include_outside_points` or `ignore_unknown_ids` *must* be passed in snake case or a `KeyError` will be raised.)
- The method `DatapointsAPI.insert_dataframe` has new default values for `dropna` (now `True`, still being applied on a per-column basis to not lose any data) and `external_id_headers` (now `True`, disincentivizing the use of internal IDs).
- The previous fetching logic awaited and collected all errors before raising (through the use of an "initiate-and-forget" thread pool). This is great, e.g., updates/inserts to make sure you are aware of all partial changes. However, when reading datapoints, a better option is to just fail fast (which it does now).
- `DatapointsAPI.[retrieve/retrieve_arrays/retrieve_dataframe]` no longer requires `start` (default: `0`, i.e. 1970-01-01) and `end` (default: `now`). This is now aligned with the API.
- Additionally, `DatapointsAPI.retrieve_dataframe` no longer requires `granularity` and `aggregates`.
- All retrieve methods accept a list of full query dictionaries for `id` and `external_id` giving full flexibility for all individual settings: `start`, `end`, `aggregates`, `granularity`, `limit`, `include_outside_points`, `ignore_unknown_ids`.
- Aggregates returned now include the time period(s) (given by the `granularity` unit) that `start` and `end` are part of (as opposed to only "fully in-between" points). This change is the *only breaking change* to the `DatapointsAPI.retrieve` method for aggregates and makes it so that the SDK match manual queries sent using e.g. `curl` or Postman. In other words, this is now aligned with the API.
Note also that this is a **bugfix**: Due to the SDK rounding differently than the API, you could supply `start` and `end` (with `start < end`) and still be given an error that `start is not before end`. This can no longer happen.
- Fetching raw datapoints using `include_outside_points=True` now returns both outside points (if they exist), regardless of `limit` setting (this is the *only breaking change* for limited raw datapoint queries; unlimited queries are fully backwards compatible). Previously the total number of points was capped at `limit`, thus typically only returning the first. Now up to `limit+2` datapoints are always returned. This is now aligned with the API.
- When passing a relative or absolute time specifier string like `"2w-ago"` or `"now"`, all time series in the same query will use the exact same value for 'now' to avoid any inconsistencies in the results.
- Fetching newly inserted datapoints no longer suffers from very long wait times (or timeout risk) as the code's dependency on `count` aggregates has been removed entirely (implementation detail) which could delay fetching by anything between a few seconds to several minutes/go to timeout while the aggregate was computed on-the-fly. This was mostly a problem for datapoints inserted into low-priority time periods (far away from current time).
- Asking for the same time series any number of times no longer raises an error (from the SDK), which is useful for instance when fetching disconnected time periods. This is now aligned with the API. Thus, the custom exception `CogniteDuplicateColumnsError` is no longer needed and has been removed from the SDK.
- ...this change also causes the `.get` method of `DatapointsList` and `DatapointsArrayList` to now return a list of `Datapoints` or `DatapointsArray` respectively *when duplicated identifiers are queried*. For data scientists and others used to `pandas`, this syntax is familiar to the slicing logic of `Series` and `DataFrame` when used with non-unique indices.
There is also a very subtle **bugfix** here: since the previous implementation allowed the same time series to be specified by both its `id` and `external_id`, using `.get` to access it would always yield the settings that were specified by the `external_id`. This will now return a `list` as explained above.
- `Datapoints` and `DatapointsArray` now store the `granularity` string given by the user (when querying aggregates) which allows both `to_pandas` methods (on `DatapointsList` and `DatapointsArrayList` as well) to accept `include_granularity_name` that appends this to the end of the column name(s).
- Datapoints fetching algorithm has changed from one that relies on up-to-date and correct `count` aggregates to be fast (with fallback on serial fetching when missing/unavailable), to recursively (and reactively) splitting the time-domain into smaller and smaller pieces, depending on the discovered-as-fetched density-distribution of datapoints in time and the number of available workers/threads. The new approach also has the ability to group more than 1 (one) time series per API request (when beneficial) and short-circuit once a user-given limit has been reached (if/when given). This method is now used for *all types of queries*; numeric raw-, string raw-, and aggregate datapoints.

#### Change: `retrieve_dataframe`
- Previously, fetching was constricted (🐍) to either raw- OR aggregate datapoints. This restriction has been lifted and the method now works exactly like the other retrieve-methods (with a few extra options relevant only for pandas `DataFrame`s).
- Used to fetch time series given by `id` and `external_id` separately - this is no longer the case. This gives a significant, additional speedup when both are supplied.
- The `complete` parameter has been removed and partially replaced by `uniform_index (bool)` which covers a subset of the previous features (with some modifications: now gives a uniform index all the way from the first given `start` to the last given `end`). Rationale: Old method had a weird and had unintuitive syntax (passing a string using commas to separate options).
- Interpolating, forward-filling or in general, imputation (also prev. controlled via the `complete` parameter) is completely removed as the resampling logic *really* should be up to the user fetching the data to decide, not the SDK.
- New parameter `column_names` (as already used in several existing `to_pandas` methods) decides whether to pick `id`s or `external_id`s as the dataframe column names. Previously, when both were supplied, the dataframe ended up with a mix.
Read more below in the removed section or check out the method's updated documentation.
- The ordering of columns for aggregates is now always chronological instead of the somewhat arbitrary choice made in `Datapoints.__init__`, (since `dict`s keep insertion order in newer python versions and instance variables lives in `__dict__`)).
- New parameter `include_granularity_name` that appends the specified granularity to the column names if passed as `True`. Mimics the behaviour of the older, well-known argument `include_aggregate_name`, but adds after: `my-ts|average|13m`.

### Fixed
- `CogniteClientMock` has been updated with 24 missing APIs (including sub-composited APIs like `FunctionsAPI.schedules`) and is now used internally in testing instead of a similar, additional implementation.
- Loads of `assert`s meant for the SDK user have been changed to raising exceptions instead as a safeguard since `assert`s are ignored when running in optimized mode `-O` (or `-OO`).

### Fixed: Extended time domain
- `TimeSeries.[first/count/latest]()` now work with the expanded time domain (minimum age of datapoints was moved from 1970 to 1900, see [4.2.1]).
  - `TimeSeries.latest()` now supports the `before` argument similar to `DatapointsAPI.retrieve_latest`.
  - `TimeSeries.first()` now considers datapoints before 1970 and after "now".
  - `TimeSeries.count()` now considers datapoints before 1970 and after "now" and will raise an error for string time series as `count` (or any other aggregate) is not defined.
- `DatapointsAPI.retrieve_latest` would give latest datapoint `before="now"` when given `before=0` (1970) because of a bad boolean check. Used to not be a problem since there were no data before epoch.
- The utility function `ms_to_datetime` no longer raises `ValueError` for inputs from before 1970, but will raise for input outside the allowed minimum- and maximum supported timestamps in the API.
**Note**: that support for `datetime`s before 1970 may be limited on Windows, but `ms_to_datetime` should still work (magic!).

### Fixed: Datapoints-related
- **Critical**: Fetching aggregate datapoints now works properly with the `limit` parameter. In the old implementation, `count` aggregates were first fetched to split the time domain efficiently - but this has little-to-no informational value when fetching *aggregates* with a granularity, as the datapoints distribution can take on "any shape or form". This often led to just a few returned batches of datapoints due to miscounting (e.g. as little as 10% of the actual data could be returned(!)).
- Fetching datapoints using `limit=0` now returns zero datapoints, instead of "unlimited". This is now aligned with the API.
- Removing aggregate names from the columns in a Pandas `DataFrame` in the previous implementation used `Datapoints._strip_aggregate_name()`, but this had a bug: Whenever raw datapoints were fetched all characters after the last pipe character (`|`) in the tag name would be removed completely. In the new version, the aggregate name is only added when asked for.
- The method `Datapoints.to_pandas` could return `dtype=object` for numeric time series when all aggregate datapoints were missing; which is not *that* unlikely, e.g., when using `interpolation` aggregate on a `is_step=False` time series with datapoints spacing above one hour on average. In such cases, an object array only containing `None` would be returned instead of float array dtype with `NaN`s. Correct dtype is now enforced by an explicit `pandas.to_numeric()` cast.
- Fixed a bug in all `DatapointsAPI` retrieve-methods when no time series was/were found, a single identifier was *not* given (either list of length 1 or all given were missing), `ignore_unknown_ids=True`, and `.get` was used on the empty returned `DatapointsList` object. This would raise an exception (`AttributeError`) because the mappings from `id` or `external_id` to `Datapoints` were not defined on the object (only set when containing at least 1 resource).

### Removed
- Method: `DatapointsAPI.query`. No longer needed as all "optionality" has been moved to the three `retrieve` methods.
- Method: `DatapointsAPI.retrieve_dataframe_dict`. Rationale: Due to its slightly confusing syntax and return value, it basically saw no use "in the wild".
- Custom exception: `CogniteDuplicateColumnsError`. No longer needed as the retrieve endpoints now support duplicated identifiers to be passed (similar to the API).
- All convenience methods related to plotting and the use of `matplotlib`. Rationale: No usage and low utility value: the SDK should not be a data science library.

## [4.11.3] - 2022-11-02
### Fixed
- Fix FunctionCallsAPI filtering

## [4.11.2] - 2022-11-16
### Changed
- Detect endpoint (for Engineering Diagram detect jobs) is updated to spawn and handle multiple jobs.
### Added
- `DetectJobBundle` dataclass: A way to manage multiple files and jobs.

## [4.11.1] - 2022-11-08
### Changed
- Update doc for Vision extract method
- Improve error message in `VisionExtractJob.save_annotations`

## [4.11.0] - 2022-10-17
### Added
- Add `compute` method to `cognite.client.geospatial`

## [4.10.0] - 2022-10-11
### Added
- Add `retrieve_latest` method to `cognite.client.sequences`
- Add support for extending the expiration time of download links returned by `cognite.client.files.retrieve_download_urls()`

## [4.9.0] - 2022-10-10
### Added
- Add support for extraction pipeline configuration files
### Deprecated
- Extraction pipeline runs has been moved from `client.extraction_pipeline_runs` to `client.extraction_pipelines.runs`

## [4.8.1] - 2022-10-06
### Fixed
- Fix `__str__` method of `TransformationSchedule`

## [4.8.0] - 2022-09-30
### Added
- Add operations for geospatial rasters

## [4.7.1] - 2022-09-29
### Fixed
- Fixed the `FunctionsAPI.create` method for Windows-users by removing
  validation of `requirements.txt`.

## [4.7.0] - 2022-09-28
### Added
- Support `tags` on `transformations`.

## [4.6.0] - 2022-09-26
### Changed
- Change geospatial.aggregate_features to support `aggregate_output`

## [4.5.4] - 2022-09-19
### Fixed
- The raw rows insert endpoint is now subject to the same retry logic as other idempotent endpoints.

## [4.5.3] - 2022-09-15
### Fixed
- Fixes the OS specific issue where the `requirements.txt`-validation failed
  with `Permission Denied` on Windows.

## [4.5.2] - 2022-09-09
### Fixed
- Fixes the issue when updating transformations with new nonce credentials

## [4.5.1] - 2022-09-08
### Fixed
- Don't depend on typing_extensions module, since we don't have it as a dependency.

## [4.5.0] - 2022-09-08
### Added
- Vision extract implementation, providing access to the corresponding [Vision Extract API](https://docs.cognite.com/api/v1/#tag/Vision).

## [4.4.3] - 2022-09-08
### Fixed
- Fixed NaN/NA value check in geospatial FeatureList

## [4.4.2] - 2022-09-07
### Fixed
- Don't import numpy in the global space in geospatial module as it's an optional dependency

## [4.4.1] - 2022-09-06
### Fixed
- Fixed FeatureList.from_geopandas to handle NaN values

## [4.4.0] - 2022-09-06
### Changed
- Change geospatial.aggregate_features to support order_by

## [4.3.0] - 2022-09-02
### Added
- Add geospatial.list_features

## [4.2.1] - 2022-08-23
### Changed
- Change timeseries datapoints' time range to start from 01.01.1900

## [4.2.0] - 2022-08-23
### Added
- OAuthInteractive credential provider. This credential provider will redirect you to a login page
and require that the user authenticates. It will also cache the token between runs.
- OAuthDeviceCode credential provider. Display a device code to enter into a trusted device.
It will also cache the token between runs.

## [4.1.2] - 2022-08-22
### Fixed
- geospatial: support asset links for features

## [4.1.1] - 2022-08-19
### Fixed
- Fixed the issue on SDK when Python installation didn't include pip.

### Added
- Added Optional dependency called functions. Usage: `pip install "cognite-sdk[functions]"`

## [4.1.0] - 2022-08-18
### Added
- ensure_parent parameter to client.raw.insert_dataframe method

## [4.0.1] - 2022-08-17
### Added
- OAuthClientCredentials now supports token_custom_args.

## [4.0.0] - 2022-08-15
### Changed
- Client configuration no longer respects any environment variables. There are other libraries better
suited for loading configuration from the environment (such as builtin `os` or `pydantic`). There have also
been several reports of ennvar name clash issues in tools built on top the SDK. We therefore
consider this something that should be handled by the application consuming the SDK. All configuration of
`cognite.client.CogniteClient` now happens using a `cognite.client.ClientConfig` object. Global configuration such as
`max_connection_pool_size` and other options which apply to all client instances are now configured through
the `cognite.client.global_config` object which is an instance of `cognite.client.GlobalConfig`. Examples
have been added to the docs.
- Auth has been reworked. The client configuration no longer accepts the `api_key` and `token_...` arguments.
It accepts only a single `credentials` argument which must be a `CredentialProvider` object. A few
implementations have been provided (`APIKey`, `Token`, `OAuthClientCredentials`). Example usage has
been added to the docs. More credential provider implementations will be added in the future to accommodate
other OAuth flows.

### Fixed
- A bug in the Functions SDK where the lifecycle of temporary files was not properly managed.

## [3.9.0] - 2022-08-11
### Added
- Moved Cognite Functions from Experimental SDK to Main SDK.

## [3.8.0] - 2022-08-11
### Added
- Add ignore_unknown_ids parameter to sequences.retrieve_multiple

## [3.7.0] - 2022-08-10
### Changed
- Changed grouping of Sequence rows on insert. Each group now contains at most 100k values and at most 10k rows.

## [3.6.1] - 2022-08-10
### Fixed
- Fixed a minor casing error for the geo_location field on files

## [3.6.0] - 2022-08-10
### Added
- Add ignore_unknown_ids parameter to files.retrieve_multiple

## [3.5.0] - 2022-08-10
### Changed
- Improve type annotations. Use overloads in more places to help static type checkers.

## [3.4.3] - 2022-08-10
### Changed
- Cache result from pypi version check so it's not executed for every client instantiation.

## [3.4.2] - 2022-07-28
### Fixed
- Fix the wrong destination name in transformations.

## [3.4.1] - 2022-07-27
### Fixed
- fixed exception when printing exceptions generated on transformations creation/update.

## [3.4.0] - 2022-07-21
### Added
- added support for nonce authentication on transformations

### Changed
- if no source or destination credentials are provided on transformation create, an attempt will be made to create a session with the CogniteClient credentials, if it succeeds, the acquired nonce will be used.
- if OIDC credentials are provided on transformation create/update, an attempt will be made to create a session with the given credentials. If it succeeds, the acquired nonce credentials will replace the given client credentials before sending the request.

## [3.3.0] - 2022-07-21
### Added
- added the sessions API

## [3.2.0] - 2022-07-15
### Removed
- Unused cognite.client.experimental module

## [3.1.0] - 2022-07-13
### Changed
- Helper functions for conversion to/from datetime now warns on naive datetimes and their interpretation.
### Fixed
- Helper function `datetime_to_ms` now accepts timezone aware datetimes.

## [3.0.1] - 2022-07-13
### Fixed
- fixed missing README.md in package

## [3.0.0] - 2022-07-12
### Changed
- Poetry build, one single package "cognite-sdk"
- Require python 3.8 or greater (used to be 3.5 or greater)
### Removed
- support for root_asset_id and root_asset_external_id filters. use asset subtree filters instead.

## [2.56.1] - 2022-06-22
### Added
- Time series property `is_step` can now be updated.

## [2.56.0] - 2022-06-21
### Added
- added the diagrams API

## [2.55.0] - 2022-06-20

### Fixed
- Improve geospatial documentation and implement better parameter resilience for filter and feature type update

## [2.54.0] - 2022-06-17

### Added
- Allow to set the chunk size when creating or updating geospatial features

## [2.53.1] - 2022-06-17

### Fixed
- Fixed destination type decoding of `transformation.destination`

## [2.53.0] - 2022-06-16

### Added
- Annotations implementation, providing access to the corresponding [Annotations API](https://docs.cognite.com/api/v1/#tag/Annotations).
    - Added `Annotation`, `AnnotationFilter`, `AnnotationUpdate` dataclasses to `cognite.client.data_classes`
    - Added `annotations` API to `cognite.client.CogniteClient`
    - **Create** annotations with `client.annotations.create` passing `Annotation` instance(s)
    - **Suggest** annotations with `client.annotations.suggest` passing `Annotation` instance(s)
    - **Delete** annotations with `client.annotations.delete` passing the id(s) of annotation(s) to delete
    - **Filter** annotations with `client.annotations.list` passing a `AnnotationFilter `dataclass instance or a filter `dict`
    - **Update** annotations with `client.annotations.update` passing updated `Annotation` or `AnnotationUpdate` instance(s)
    - **Get single** annotation with `client.annotations.retrieve` passing the id
    - **Get multiple** annotations with `client.annotations.retrieve_multiple` passing the ids

## [2.52.0] - 2022-06-10
### Changed
- Reverted the optimizations introduced to datapoints fetching in 2.47.0 due to buggy implementation.

## [2.51.0] - 2022-06-10
### Added
- added the new geo_location field to the Asset resource

## [2.50.2] - 2022-06-09
### Fixed
- Geospatial: fix FeatureList.from_geopandas issue with optional properties

## [2.50.1] - 2022-06-07
### Fixed
- Geospatial: keep feature properties as is

## [2.50.0] - 2022-05-27
### Changed
- Geospatial: deprecate update_feature_types and add patch_feature_types

## [2.49.1] - 2022-05-19
### Changed
- Geospatial: Support dataset

## [2.49.0] - 2022-05-09
### Changed
- Geospatial: Support output selection for getting features by ids

## [2.48.0] - 2022-05-09
### Removed
- Experimental model hosting API

## [2.47.0] - 2022-05-02
### Changed
- Performance gain for `datapoints.retrieve` by grouping together time series in single requests against the underlying API.

## [2.46.1] - 2022-04-22
### Changed
- POST requests to the `sessions/revoke`-endpoint are now automatically retried

## [2.46.0] - 2022-03-29
### Changed
- Fix retrieval of empty raster in experimental geospatial api: http 204 as ok status

## [2.45.0] - 2022-03-25
### Added
- support `sequence_rows` destination type on Transformations.

## [2.44.1] - 2022-03-24
### Fixed
- fix typo in `data_set_ids` parameter type on `transformations.list`.

## [2.44.0] - 2022-03-24
### Added
- support conflict mode parameter on `transformations.schema.retrieve`.

## [2.43.1] - 2022-03-24
### Added
- update pillow dependency 9.0.0 -> 9.0.1

## [2.43.0] - 2022-03-21
### Added
- new list parameters added to `transformations.list`.

## [2.42.0] - 2022-02-11
### Added
- FeatureList.from_geopandas() improvements

## [2.41.3] - 2022-02-11
### Fixed
- example for templates view.

## [2.41.2] - 2022-02-16
### Added
- support for deleting properties and search specs in GeospatialAPI.update_feature_types(...).

## [2.40.1] - 2022-02-15
### Fixed
- geospatial examples.

## [2.40.0] - 2022-02-11
### Added
- dataSetId support for transformations.

## [2.39.1] - 2022-01-25
### Added
- pandas and geospatial dependencies optional for cognite-sdk-core.

## [2.39.0] - 2022-01-20
### Added
- geospatial API support

## [2.38.6] - 2021-12-17
### Added
- add the possibility to cancel transformation jobs.

## [2.38.5] - 2022-01-12
### Fixed
- Bug where creating/updating/deleting more than 5 transformation schedules in a single call would fail.

## [2.38.4] - 2021-12-17
### Fixed
- Bug where list generator helper will return more than chunk_size items.

## [2.38.3] - 2021-12-13
### Fixed
- Bug where client consumes all streaming content when logging request.

## [2.38.2] - 2021-12-09
### Added
- add the possibility to pass extra body fields to APIClient._create_multiple.

## [2.38.1] - 2021-12-07
### Fixed
- Bug where loading `transformations.jobs` from JSON fails for raw destinations.

## [2.38.0] - 2021-11-30
### Added
- `transformations` api client, which allows the creation, deletion, update, run and retrieval of transformations.
- `transformations.schedules` api client, which allows the schedule, unschedule and retrieval of recurring runs of a transformation.
- `transformations.notifications` api client, which allows the creation, deletion and retrieval of transformation email notifications.
- `transformations.schema` api client, which allows the retrieval of the expected schema of sql transformations based on the destination data type.
- `transformations.jobs` api client, which retrieves the  status of transformation runs.

## [2.37.1] - 2021-12-01
### Fixed
- Bug where `sequences` full update attempts to "set" column spec. "set" is not supported for sequence column spec.

## [2.37.0] - 2021-11-30
### Added
- Added support for retrieving file download urls

## [2.36.0] - 2021-11-30
### Fixed
- Changes default JSON `.dumps()` behaviour to be in strict compliance with the standard: if any NaNs or +/- Infs are encountered, an exception will now be raised.

## [2.35.0] - 2021-11-29
### Added
- Added support for `columns` update on sequences

## [2.34.0] - 2021-11-5
### Added
- Added support for `data_set_id` on template views

## [2.33.0] - 2021-10-27
### Security
- Disallow downloading files to path outside download directory in `files.download()`.

## [2.32.0] - 2021-10-04
### Added
 - Support for extraction pipelines

## [2.31.1] - 2021-09-27
### Fixed
- Fixed a bug related to handling of binary response payloads.

## [2.31.0] - 2021-08-26
### Added
- View resolver for template fields.

## [2.30.0] - 2021-08-18
### Added
- Support for Template Views

## [2.29.0] - 2021-08-16
### Added
- Raw rows are retrieved using parallel cursors when no limit is set.

## [2.28.2] - 2021-08-10
### Added
- Relationships now supports `partitions` parameter for [parallel retrieval](https://docs.cognite.com/api/v1/#section/Parallel-retrieval)

## [2.28.1] - 2021-08-10
### Changed
- debug mode now logs response payload and headers.

## [2.27.0] - 2021-07-20

### Fixed
- When using CogniteClient with the client-secret auth flow, the object would not be pickle-able (e.g. when using multiprocessing) because of an anonymous function.

## [2.26.1] - 2021-07-20

### Changed
- Optimization. Do not get windows if remaining data points is 0. Reduces number of requests when asking for 100k data points/10k aggregates from 2 to 1.

## [2.26.0] - 2021-07-08

### Added
- Support for set labels on AssetUpdate

## [2.25.0] - 2021-07-06

### Added
- filter_nodes function to ThreeDRevisionsAPI

## [2.24.0] - 2021-06-28

### Added
- ignore_unknown_ids flag to Relationships delete method

## [2.23.0] - 2021-06-25

### Added
- insert_dataframe and retrieve_dataframe methods to the Raw client

## [2.22.0] - 2021-06-22

### Added
- More contextualization job statuses
### Changed
- Refactor contextualization constant representation

## [2.21.0] - 2021-06-18

### Added
- Datasets support for labels

## [2.20.0] - 2021-06-04

### Added
- rows() in RawRowsAPI support filtering with `columns` and `min/maxLastUpdatedTime`

## [2.19.0] - 2021-05-06

### Added
- Support for /token/inspect endpoint

## [2.18.2] - 2021-04-23

### Fixed
- Bug in templates instances filter that would cause `template_names` to be ignored.

## [2.18.1] - 2021-04-22

### Added
- Configure file download/upload timeouts with `COGNITE_FILE_TRANSFER_TIMEOUT` environment variable or
`file_transfer_timeout` parameter on `CogniteClient`.

### Changed
- Increased default file transfer timeout from 180 to 600 seconds
- Retry more failure modes (read timeouts, 502, 503, 504) for files upload/download requests.

## [2.18.0] - 2021-04-20

### Changed
- `COGNITE_DISABLE_SSL` now also covers ssl verification on IDP endpoints used for generating tokens.


## [2.17.1] - 2021-04-13

### Added
- `created_time`, and `last_updated_time` to template data classes.
- `data_set_id` to template instance data class.


## [2.17.0] - 2021-03-26

### Changed
- Ignore exceptions from pypi version check and reduce its timeout to 5 seconds.

### Fixed
- Only 200/201/202 is treated as successful response. 301 led to json decoding errors -
now handled gracefully.
- datasets create limit was set to 1000 in the sdk, leading to cases of 400 from the api where the limit is 10.

### Added
- Support for specifying proxies in the CogniteClient constructor

### Removed
- py.typed file. Will not declare library as typed until we run a typechecker on the codebase.


## [2.16.0] - 2021-03-24

### Added
- support for templates.
- date-based `cdf-version` header.

## [2.15.0] - 2021-03-12

### Added
- `createdTime` field on raw dbs and tables.

## [2.14.0] - 2021-03-18

### Added
- dropna argument to insert_dataframe method in DatapointsAPI

## [2.13.0] - 2021-03-12

### Added
- `sortByNodeId` and `partitions` query parameters to `list_nodes` method.

## [2.12.2] - 2021-03-11

### Fixed
- CogniteAPIError raised (instead of internal KeyError) when inserting a RAW row without a key.

## [2.12.1] - 2021-03-09

### Fixed
- CogniteMissingClientError raised when creating relationship with malformed body.

## [2.12.0] - 2021-03-04

### Changed
- Move Entity matching API from beta to v1.

## [2.11.1] - 2021-02-18

### Changed
- Resources are now more lenient on which types they accept in for labels

## [2.11.0] - 2021-02-18

### Changed
- Entity matching fit will flatten dictionaries and resources to "metadata.subfield" similar to pipelines.

### Added
- Relationships now support update

## [2.10.7] - 2021-02-02

### Fixed
- Relationships API list calls via the generator now support `chunk_size` as parameter.

## [2.10.6] - 2021-02-02

### Fixed
- Retry urllib3.NewConnectionError when it isn't in the context of a ConnectionRefusedError

## [2.10.5] - 2021-01-25

### Fixed
- Fixed asset subtree not returning an object with id->item cache for use in .get

## [2.10.4] - 2020-12-14

### Changed
- Relationships filter will now chain filters on large amounts of sources or targets in batches of 1000 rather than 100.


## [2.10.3] - 2020-12-09

### Fixed
- Retries now have backup time tracking per request, rather than occasionally shared between threads.
- Sequences delete ranges now no longer gives an error if no data is present

## [2.10.2] - 2020-12-08

### Fixed
- Set geoLocation.type in files to "Feature" if missing

## [2.10.1] - 2020-12-03

### Added
- Chaining of requests to the relationships list method,
allowing the method to take arbitrarily long lists for `source_external_ids` and `target_external_ids`

## [2.10.0] - 2020-12-01

### Added
- Authentication token generation and lifecycle management

## [2.9.0] - 2020-11-25

### Added
- Entity matching API is now available in the beta client.

## [2.8.0] - 2020-11-23

### Changed
- Move relationships to release python SDK

## [2.7.0] - 2020-11-10

### Added
- `fetch_resources` parameter to the relationships `list` and `retrieve_multiple` methods, which attempts to fetch the resource referenced in the relationship.

## [2.6.4] - 2020-11-10

### Fixed
- Fixed a bug where 429 was not retried on all endpoints

## [2.6.3] - 2020-11-06

### Fixed
- Resource metadata should be able to set empty using `.metadata.set(None)` or `.metadata.set({})`.

## [2.6.2] - 2020-11-05

### Fixed
- Asset retrieve subtree should return empty AssetList if asset does not exist.

## [2.6.1] - 2020-10-30

### Added
- `geospatial` to list of valid relationship resource types.

## [2.6.0] - 2020-10-26

### Changed
- Relationships list should take dataset internal and external id as different parameters.

## [2.5.4] - 2020-10-22

### Fixed
- `_is_retryable` didn't handle clusters with a dash in the name.

## [2.5.3] - 2020-10-14

### Fixed
- `delete_ranges` didn't cast string timestamp into number properly.

## [2.5.2] - 2020-10-06

### Fixed
- `labels` in FileMetadata is not cast correctly to a list of `Label` objects.

## [2.5.1] - 2020-10-01
- Include `py.typed` file in sdk distribution

## [2.5.0] - 2020-09-25

### Added
- Relationships beta support.

### Removed
- Experimental Model Hosting client.

## [2.4.3] - 2020-09-18
- Increase raw rows list limit to 10,000

## [2.4.2] - 2020-09-10
- Fixed a bug where urls with query parameters were excluded from the retryable endpoints.

## [2.4.1] - 2020-09-09

### Changed
- Generator-based listing now supports partitions. Example:
  ``` python
  for asset in client.assets(partitions=10):
    # do something
  ```

## [2.4.0] - 2020-08-31

### Added
- New 'directory' in Files

## [2.3.0] - 2020-08-25

### Changed
- Add support for mypy and other type checking tools by adding packaging type information

## [2.2.2] - 2020-08-18

### Fixed
- HTTP transport logic to better handle retrying of connection errors
- read timeouts will now raise a CogniteReadTimeout
- connection errors will now raise a CogniteConnectionError, while connection refused errors will raise the more
 specific CogniteConnectionRefused exception.

### Added
- Jitter to exponential backoff on retries

### Changed
- Make HTTP requests no longer follow redirects by default
- All exceptions now inherit from CogniteException

## [2.2.1] - 2020-08-17

### Added
- Fixed a bug where `/timeseries/list` was missing from the retryable endpoints.

## [2.2.0] - 2020-08-14

### Added
- Files labelling support

## [2.1.2] - 2020-08-13

### Fixed
- Fixed a bug where only v1 endpoints (not playground) could be added as retryable

## [2.1.1] - 2020-08-04

### Fixed
- Calls to datapoints `retrieve_dataframe` with `complete="fill"` would break using Pandas version 1.1.0 because it raises TypeError when calling `.interpolate(...)` on a dataframe with no columns.

## [2.1.0] - 2020-07-22

### Added
- Support for passing a single string to `AssetUpdate().labels.add` and `AssetUpdate().labels.remove`. Both a single string and a list of strings is supported. Example:
  ```python
  # using a single string
  my_update = AssetUpdate(id=1).labels.add("PUMP").labels.remove("VALVE")
  res = c.assets.update(my_update)

  # using a list of strings
  my_update = AssetUpdate(id=1).labels.add(["PUMP", "ROTATING_EQUIPMENT"]).labels.remove(["VALVE"])
  res = c.assets.update(my_update)
  ```

## [2.0.0] - 2020-07-17

### Changed
- The interface to interact with labels has changed. A new, improved interface is now in place to make it easier to work with CDF labels. The new interface behaves this way:
  ```python
  # crate label definition(s)
  client.labels.create(LabelDefinition(external_id="PUMP", name="Pump", description="Pump equipment"))
  # ... or multiple
  client.labels.create([LabelDefinition(external_id="PUMP"), LabelDefinition(external_id="VALVE")])

  # list label definitions
  label_definitions = client.labels.list(name="Pump")

  # delete label definitions
  client.labels.delete("PUMP")
  # ... or multiple
  client.labels.delete(["PUMP", "VALVE"])

  # create an asset with label
  asset = Asset(name="my_pump", labels=[Label(external_id="PUMP")])
  client.assets.create(assets)

  # filter assets by labels
  my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])
  asset_list = client.assets.list(labels=my_label_filter)

  # attach/detach labels to/from assets
  my_update = AssetUpdate(id=1).labels.add(["PUMP"]).labels.remove(["VALVE"])
  res = c.assets.update(my_update)
  ```

### Fixed
- Fixed bug where `_call_` in SequencesAPI (`client.sequences`) was incorrectly returning a `GET` method instead of `POST`.

## [1.8.1] - 2020-07-07
### Changed
- For 3d mappings delete, only use node_id and asset_id pairs in delete request to avoid potential bad request.
- Support attaching/detaching multiple labels on assets in a single method

## [1.8.0] - 2020-06-30
### Added
- Synthetic timeseries endpoint for DatapointsApi
- Labels endpoint support
- Assets labelling support
- Support for unique value aggregation for events.

### Changed
- When `debug=true`, redirects are shown more clearly.

## [1.7.0] - 2020-06-03
### Fixed
- datasetId is kept as an integer in dataframes.

### Changed
- Internal list of retryable endpoints was changed to a class variable so it can be modified.

## [1.6.0] - 2020-04-28
### Added
- Support events filtering by ongoing events (events without `end_time` defined)
- Support events filtering by active timerange of event
- Support files metadata filtering by `asset_external_ids`
- Aggregation endpoint for Assets, DataSets, Events, Files, Sequences and TimeSeries API

## [1.5.2] - 2020-04-02
### Added
- Support for security categories on file methods

## [1.5.1] - 2020-04-01
### Added
- Support for security categories on files
- active_at_time on relationships

### Fixed
- No longer retry calls to /files/initupload
- Retry retryable POST endpoints in datasets API

## [1.5.0] - 2020-03-12
### Added
- DataSets API and support for this in assets, events, time series, files and sequences.
- .asset helper function on time series.
- asset external id filter on time series.

## [1.4.13] - 2020-03-03
### Added
- Relationship list supports multiple sources, targets, relationship types and datasets.

## [1.4.12] - 2020-03-02

### Fixed
- Fixed a bug in file uploads where fields other than name were not being passed to uploaded directories.

## [1.4.11] - 2020-02-21

### Changed
- Datapoint insertion changed to be less memory intensive.

### Fixed
- Fixed a bug where add service account to group expected items in response.
- Jupyter notebook output and non-camel cased to_pandas uses nullable int fields instead of float for relevant fields.

## [1.4.10] - 2020-01-24
### Added
- Support for the error field for synthetic time series query in the experimental client.
- Support for retrieving data from multiple sequences at once.

## [1.4.9] - 2019-12-19

### Fixed
- Fixed a bug where datapoints `retrieve` could return less than limit even if there were more datapoints.
- Fixed an issue where `insert_dataframe` would give an error with older pandas versions.

## [1.4.8] - 2019-12-19

### Added
- Support for `ignore_unknown_ids` on time series `retrieve_multiple`, `delete` and datapoints `retrieve` and `latest` and related endpoints.
- Support for asset subtree filters on files, sequences, and time series.
- Support for parent external id filters on assets.
- Synthetic datapoints retrieve has additional functions including variable replacement and sympy support.

### Changed
- Synthetic datapoints now return errors in the `.error` field, in the jupyter output, and optionally in pandas dataframes if `include_errors` is set.

## [1.4.7] - 2019-12-05

### Added
- Support for synthetic time series queries in the experimental client.
- parent external id filter added for assets.

### Fixed
- startTime in event dataframes is now a nullable int dtype, consistent with endTime.

## [1.4.6] - 2019-12-02

### Fixed
- Fixed notebook output for Asset, Datapoint and Raw.

## [1.4.5] - 2019-12-02

### Changed

- The ModelHostingAPI now calls Model Hosting endpoints in playground instead of 0.6.

## [1.4.4] - 2019-11-29

### Added
 - Option to turn off version checking from CogniteClient constructor

### Changed
- In sequences create, the column definitions object accepts both camelCased and snake_cased keys.
- Retry 429 on all endpoints

### Fixed
- Fixed notebook output for DatapointsList

## [1.4.3] - 2019-11-27
### Fixed
- In Jupyter notebooks, the output from built-in list types is no longer camel cased.

## [1.4.2] - 2019-11-27

### Changed
- In the 3D API, the call and list methods now include all models by default instead of only unpublished ones.
- In Jupyter notebooks, the output from built-in types is no longer camel cased.

### Added
- Support for filtering events by asset subtree ids.

## [1.4.1] - 2019-11-18

### Added
- Support for filtering events by asset external id.
- query parameter on asset search.
- `ignore_unknown_ids` parameter on asset and events method `delete` and `retrieve_multiple`.

## [1.4.0] - 2019-11-14

### Changed
- In the ModelHostingAPI, models, versions and schedules are now referenced by name instead of id. The ids are no longer available.
- In the ModelHostingAPI, functions related to model versions are moved from the ModelsAPI to the new ModelVersionsAPI.
- In the ModelHostingAPI, the model names must be unique. Also, the version names and schedule names must be unique per model.
- Default value for `limit` in search method is now 100 instead of None to clarify api default behaviour when no limit is passed.

## [1.3.4] - 2019-11-07

### Changed
- Error 500's are no longer retried by default, only HTTP 429, 502, 503, 504 are.
- Optimized HTTP calls by caching user agent.
- Relationship filtering is now integrated into `list` instead of `search`.
- Sequences `insert_dataframe` parameter `external_id_headers` documentation updated.
- Type hints for several objects formerly `Dict[str, Any]` improved along with introducing matching dict derived classes.

### Fixed
- `source_created_time` and `source_modified_time` on files now displayed as time fields.
- Fixed pagination for `include_outside_points` and other edge cases in datapoints.
- Fixed a bug where `insert_dataframe` with strings caused a numpy error.

### Added
- Relationships can now have sequences as source or target.

## [1.3.3] - 2019-10-21

### Changed
- Datapoints insert dataframe function will check for infinity values.
- Allow for multiple calls to .add / .remove in object updates such as metadata, without later calls overwriting former.
- List time series now ignores the include_metadata parameter.

### Added
- Advanced list endpoint is used for listing time series, adding several new filters and partitions.

## [1.3.2] - 2019-10-16

### Added
- Datapoints objects now store is_string, is_step and unit to allow for better interpretation of the data.
- Sorting when listing events
- Added a search function in the relationships API.

### Changed
- `list` and `__call__` methods for files now support list parameters for `root_ids`, `root_external_ids`.
- retrieve_dataframe with `complete` using Datapoints fields instead of retrieving time series metadata.

### Fixed
- Fixed chunking logic in list_generator to always return last partial chunk.
- Fixed an error on missing target/source in relationships.

## [1.3.1] - 2019-10-09
### Fixed
- Fixed support for totalVariation aggregate completion.
- Changed conversion of raw RowList to pandas DataFrame to handle missing values (in columns) across the rows. This also fixes the bug where one-off values would be distributed to all rows in the DataFrame (unknown bug).

## [1.3.0] - 2019-10-03
### Changed
- Sequences officially released and no longer considered experimental.
- Sequences data insert no longer takes a default value for columns.

## [1.2.1] - 2019-10-01
### Fixed
- Tokens are sent with the correct "Authorization" header instead of "Authentication".

## [1.2.0] - 2019-10-01
### Added
- Support for authenticating with bearer tokens. Can now supply a jwt or jwt-factory to CogniteClient. This token will override any api-key which has been set.

## [1.1.12] - 2019-10-01
### Fixed
- Fixed a bug in time series pagination where getting 100k datapoints could cause a missing id error when using include_outside_points.
- SequencesData `to_pandas` no longer returns NaN on integer zero columns.
- Fixed a bug where the JSON encoder would throw circular reference errors on unknown data types, including numpy floats.

## [1.1.11] - 2019-09-23
### Fixed
- Fix testing.CogniteClientMock so it is possible to get attributes on child which have not been explicitly in the CogniteClientMock constructor

## [1.1.10] - 2019-09-23
### Fixed
- Fix testing.CogniteClientMock so it is possible to get child mock not explicitly defined

### Added
- `list` and `__call__` methods for events now support list parameters for `root_asset_ids`, `root_asset_external_ids`.

## [1.1.9] - 2019-09-20
### Changed
- Renamed testing.mock_cognite_client to testing.monkeypatch_cognite_client

### Added
- testing.CogniteClientMock object

## [1.1.8] - 2019-09-19
### Added
- Support for aggregated properties of assets.
- `Asset` and `AssetList` classes now have a `sequences` function which retrieves related sequences.
- Support for partitioned listing of assets and events.

### Changed
- `list` and `__call__` methods for assets now support list parameters for `root_ids`, `root_external_ids`.
- Sequences API no longer supports column ids, all relevant functions have been changed to only use external ids.

### Fixed
- Fixed a bug in time series pagination where getting 100k dense datapoints would cause a missing id error.
- Sequences retrieve functions fixed to match API change, to single item per retrieve.
- Sequences retrieve/insert functions fixed to match API change to take lists of external ids.

## [1.1.7] - 2019-09-13
### Fixed
- `testing.mock_cognite_client()` so that it still accepts arguments after exiting from mock context.

## [1.1.6] - 2019-09-12
### Fixed
- `testing.mock_cognite_client()` so that the mocked CogniteClient may accept arguments.

## [1.1.5] - 2019-09-12
### Added
- Method `files.download_to_path` for streaming a file to a specific path

## [1.1.4] - 2019-09-12
### Added
- `root_asset_ids` parameter for time series list.

### Changed
- Formatted output in jupyter notebooks for `SequenceData`.
- `retrieve_latest` function in theDatapoints API extended to support more than 100 items.
- Log requests at DEBUG level instead of INFO.

## [1.1.3] - 2019-09-05
### Changed
- Disabled automatic handling of cookies on the requests session objects

### Fixed
- `to_pandas` method on CogniteResource in the case of objects without metadata

## [1.1.2] - 2019-08-28
### Added
- `limit` parameter on sequence data retrieval.
- Support for relationships exposed through experimental client.
- `end` parameter of sequence.data retrieval and range delete accepts -1 to indicate last index of sequence.

### Changed
- Output in jupyter notebooks is now pandas-like by default, instead of outputting long json strings.

### Fixed
- id parameters and timestamps now accept any integer type including numpy.int64, so values from dataframes can be passed directly.
- Compatibility fix for renaming of sequences cursor and start/end parameters in the API.

## [1.1.1] - 2019-08-23
### Added
- `complete` parameter on `datapoints.retrieve_dataframe`, used for forward-filling/interpolating intervals with missing data.
- `include_aggregate_name` option on `datapoints.retrieve_dataframe` and `DatapointsList.to_pandas`, used for removing the `|<aggregate-name>` postfix on dataframe column headers.
- datapoints.retrieve_dataframe_dict function, which returns {aggregate:dataframe} without adding aggregate names to columns
- source_created_time and source_modified_time support for files

## [1.1.0] - 2019-08-21
### Added
- New method create_hierarchy() added to assets API.
- SequencesAPI.list now accepts an asset_ids parameter for searching by asset
- SequencesDataAPI.insert now accepts a SequenceData object for easier copying
- DatapointsAPI.insert now accepts a Datapoints object for easier copying
- helper method `cognite.client.testing.mock_cognite_client()` for mocking CogniteClient
- parent_id and parent_external_id to AssetUpdate class.

### Changed
- assets.create() no longer validates asset hierarchy and sorts assets before posting. This functionality has been moved to assets.create_hierarchy().
- AssetList.files() and AssetList.events() now deduplicate results while fetching related resources, significantly reducing memory load.

## [1.0.5] - 2019-08-15
### Added
- files.create() method to enable creating a file without uploading content.
- `recursive` parameter to raw.databases.delete() for recursively deleting tables.

### Changed
- Renamed .iteritems() on SequenceData to .items()
- raw.insert() now chunks raw rows into batches of 10,000 instead of 1,000

### Fixed
- Sequences queries are now retried if safe
- .update() in all APIs now accept a subclass of CogniteResourceList as input
- Sequences datapoint retrieval updated to use the new cursor feature in the API
- Json serializiation in `__str__()` of base data classes. Now handles Decimal and Number objects.
- Now possible to create asset hierarchy using parent external id when the parent is not part of the batch being inserted.
- `name` parameter of files.upload_bytes is now required, so as not to raise an exception in the underlying API.

## [1.0.4] - 2019-08-05
### Added
- Variety of useful helper functions for Sequence and SequenceData objects, including .column_ids and .column_external_ids properties, iterators and slice operators.
- Sequences insert_dataframe function.
- Sequences delete_range function.
- Support for external id column headers in datapoints.insert_dataframe()

### Changed
- Sequences data retrieval now returns a SequenceData object.
- Sequences insert takes its parameters row data first, and no longer requires columns to be passed.
- Sequences insert now accepts tuples and raw-style data input.
- Sequences create now clears invalid fields such as 'id' in columns specification, so sequences can more easily re-use existing specifications.
- Sequence data function now require column_ids or column_external_ids to be explicitly set, rather than both being passed through a single columns field

## [1.0.3] - 2019-07-26
### Fixed
- Renamed Model.schedule_data_spec to Model.data_spec so the field from the API will be included on the object.
- Handling edge case in Sequences pagination when last datapoint retrieved is at requested end
- Fixing data points retrieval when count aggregates are missing
- Displays unexpected fields on error response from API when raising CogniteAPIError

## [1.0.2] - 2019-07-22
### Added
- Support for model hosting exposed through experimental client

### Fixed
- Handling dynamic limits in Sequences API

## [1.0.1] - 2019-07-19
### Added
- Experimental client
- Support for sequences exposed through experimental client

## [1.0.0] - 2019-07-11
### Added
- Support for all endpoints in Cognite API
- Generator with hidden cursor for all resource types
- Concurrent writes for all resources
- Distribution of "core" sdk which does not depend on pandas and numpy
- Typehints for all methods
- Support for posting an entire asset hierarchy, resolving ref_id/parent_ref_id automatically
- config attribute on CogniteClient to view current configuration.

### Changed
- Renamed methods so they reflect what the method does instead of what http method is used
- Updated documentation with automatically tested examples
- Renamed `stable` namespace to `api`
- Rewrote logic for concurrent reads of datapoints
- Renamed CogniteClient parameter `num_of_workers` to `max_workers`

### Removed
- `experimental` client in order to ensure sdk stability.
