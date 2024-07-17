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

## [7.54.3] - 2024-07-17
### Fixed
- Reintroduced `ListablePropertyType` that was removed in `7.37.0`. This is used to represent all container properties
  that can be listed. This is an internal change and should not affect users of the SDK, but might affect developers
  that have created libraries that depend on the SDK.
- [Feature Preview] Support for `enum` as container property type in the data modeling APIs. Note that this is not
  yet supported in the API, and is an experimental feature that may change without warning.

## [7.54.2] - 2024-07-16
### Fixed
- A bug in the list method of the RelationshipsAPI that could cause a thread deadlock.

## [7.54.1] - 2024-07-15
### Fixed
- Calling `client.functions.retrieve` or `client.functions.delete` with more than 10 ids no longer
  raises a `CogniteAPIError`.
- Iterating over functions using `client.functions` or `client.functions(...)` no longer raises a `CogniteAPIError`.
### Added
- Added missing filter parameter `metadata` to `client.functions.list`.
### Changed
- When creating a new function without specifying `description` or `owner`, the default values are now
  correctly set to `None` instead of `""`.

## [7.54.0] - 2024-07-12
### Added
- In the `client.data_modeling.instances` the methods `.search`, `.retrieve`,`.list`, `.query`, and `.sync` now
  support the `include_typing` parameter. This parameter is used to include typing information in the response,
  that can be accessed via the `.typing` attribute on the result object.

## [7.53.4] - 2024-07-11
### Added
- `FilesAPI.upload_bytes` and `FilesAPI.upload` are updated to be compatible with Private Link projects.

## [7.53.3] - 2024-07-11
### Added
- [Feature Preview - alpha] Support for `instanceId` in the `client.time_series` `.retrieve`, `.retrieve_multiple`,
  and `.update` methods. This is an experimental feature and may change without warning.

## [7.53.2] - 2024-07-03
### Fixed
- If you derived from `TypedNode` or `TypedEdge`, and then derived the `load` method would not include the parent
  class properties. Same if you used multiple inheritance. This is now fixed.
### Added
- [Feature Preview - alpha] Core Model, available `cognite.client.data_classes import cdm`.

## [7.53.1] - 2024-07-02
### Fixed
- In the new `retrieve_nodes` and `retrieve_edges` methods in the `client.data_modeling.instances` module, if you
  give the identifier of a single node or edge, you will now get a single `TypedNode` or `TypedEdge` instance back.

## [7.53.0] - 2024-07-02
### Added
- New classes `TypedNode` and `TypedEdge` (in addition to `TypedNodeApply` and `TypedEdgeApply`) to be used as
  base classes for user created classes that represent nodes and edges with properties in a specific view. For example,
  is you have a view `Person` with properties `name` and `age`, you can create a class `Person` that inherits from
  `TypedNode` and add properties `name` and `age` to it. This class can then be used with the
  `client.data_modeling.instances.retrieve(..)`, `.apply(...)`, `.list(...)` and `.search(...)` methods.

## [7.52.3] - 2024-06-27
### Added
- Added `partitions` parameter to `retrieve_dataframe()` method of the `RawRowsAPI`.

## [7.52.2] - 2024-06-26
### Added
- Alpha feature: `client.time_series.data` support for `instance_id` in `insert`, `insert_multiple`,
  `delete`, and `retrieve` methods. This is an experimental feature and may change without warning.

## [7.52.1] - 2024-06-26
### Fixed
- Calling `.extend` on a `NodeListWithCursor` or `EdgeListWithCursor` no longer raises a `TypeError`.

## [7.52.0] - 2024-06-19
### Added
- Support the `immutable` flag on container/view properties

## [7.51.1] - 2024-06-18
### Added
- Added support for serializing Node/Edge properties of type `list` of `NodeId` and `DirectRelationReference`,
  `date`, `datetime` and list of `date` and `datetime` to `json` format.

## [7.51.0] - 2024-06-16
### Added
- Support for iterating over `Functions`, `FunctionSchedules`, `DatapointSubscriptions`, `Transformations`,
  `TransformationSchedules`, `TransformationNotifications`, `ExtractionPipelines`, `Workflows`, `WorkflowVersions`.

## [7.50.0] - 2024-06-14
### Changed
- DatapointsAPI support for timezones and calendar-based aggregates reaches general availability (GA).
### Deprecated
- The function `DatapointsAPI.retrieve_dataframe_in_tz` is deprecated. Use the other retrieve methods instead
  and pass in `timezone`.

## [7.49.2] - 2024-06-12
### Fixed
- Converting rows (`RowList` and `RowListWrite`) to a pandas DataFrame no longer silently drops rows that do not have
  any columnar data.

## [7.49.1] - 2024-06-11
### Fixed
- Fixes resetting dataSetId to None in a ThreeDModelUpdate.

## [7.49.0] - 2024-06-05
### Added
- `WorkfowExecutionAPI.list` now allows filtering by execution status.

## [7.48.1] - 2024-06-04
### Fixed
- A bug introduced in `7.45.0` that would short-circuit raw datapoint queries too early when a lot of time series was
  requested at the same time, and `include_outside_points=True` was used (empty cursor are to be expected).

## [7.48.0] - 2024-06-04
### Changed
- Mark Data Workflows SDK implementation as Generally Available.

## [7.47.0] - 2024-06-04
### Added
- Support for retrieving `Labels`, `client.labels.retrieve`.

## [7.46.2] - 2024-06-03
### Added
- Added option for silencing `FeaturePreviewWarnings` in the `cognite.client.global_config`.

## [7.46.1] - 2024-05-31
### Fixed
- Pyodide issue related to missing tzdata package.

## [7.46.0] - 2024-05-31
### Added
- `RawRowsAPI.insert_dataframe` now has a new `dropna` setting (defaulting to True, as this would otherwise raise later).

## [7.45.0] - 2024-05-31
### Added
- DatapointsAPI now support `timezone` and new calendar-based granularities like `month`, `quarter` and `year`.
  These API features are in beta, and the SDK implementation in alpha, meaning breaking changes can
  occur without warning. Set beta header to avoid warning. Users of `retrieve_dataframe_in_tz` should
  consider preparing to upgrade as soon as the features reach general availability (GA).

## [7.44.1] - 2024-05-24
### Added
- Missing parameter `timeout` to `client.transformations.preview`.

## [7.44.0] - 2024-05-24
### Added
- New utility function `datetime_to_ms_iso_timestamp` in `cognite.client.utils` to convert a datetime object
  to a string representing a timestamp in the format expected by the Cognite GraphQL API.

## [7.43.6] - 2024-05-27
### Improved
- JSON is no longer attempted decoded when e.g. expecting protobuf, which currently leads to a small performance
  improvement for datapoints fetching.

## [7.43.5] - 2024-05-22
### Fixed
- Transformation schemas no longer raise when loaded into its resource type.

## [7.43.4] - 2024-05-20
### Fixed
- The data modeling APIs (Views, Containers, Data Models and Spaces) limits for create, retrieve, delete,
  and list were not matching the API spec, causing the SDK to wrongly split large calls into too few requests.
  This means that the SDK will no longer raise a `CogniteAPIError` if you, for example, try to delete
  more than 100 containers in a single method call.

## [7.43.3] - 2024-05-15
### Fixed
- Identity providers that return `expires_in` as a string no longer causes `TypeError` when authenticating.

## [7.43.2] - 2024-05-10
### Fixed
- In containers, `PropertyType` `Text` required parameter `collation` is now optional when `load()`ing, matching the API spec.

## [7.43.1] - 2024-05-10
### Fixed
- `RawRowsAPI.insert()` silently ignored rows of type `RowWriteList`.

## [7.43.0] - 2024-05-09
### Added
- Added new data classes to the contextualization module to simplify configuring diagram detect options: `DiagramDetectConfig`,`ConnectionFlags`, `CustomizeFuzziness`, `DirectionWeights`.
- `DiagramsAPI.detect()` method's parameter `configuration` now also accepts `DiagramDetectConfig` instances.

## [7.42.0] - 2024-05-06
### Changed
- Breaking change: the `workflows.executions.cancel` method now only allows cancelling one execution at a time to reflect its non-atomic operation.

## [7.41.1] - 2024-05-06
### Fixed
- An edge case when a request for datapoints from several hundred time series (with specific finite limits) would return
  more datapoints than the user-specified limit.

## [7.41.0] - 2024-04-30
### Added
- Support for Status Codes in the DatapointsAPI and DatapointSubscriptionsAPI reaches General Availability (GA).
  - You can read more in the Cognite Data Fusion developer documentation: [Status Codes reference](https://developer.cognite.com/dev/concepts/reference/quality_codes/).

## [7.40.2] - 2024-04-30
### Fixed
- `InAssetSubtree` is no longer (mistakenly) accepted as a time series filter.

## [7.40.1] - 2024-04-30
### Fixed
- Deleting multiple Datapoint Subscriptions now work as expected.

## [7.40.0] - 2024-04-30
### Added
- Datapoint Subscriptions now support status codes.

## [7.39.0] - 2024-04-25
### Added
- Support for internally managed groups (inside CDF, as opposed to the external identity provider).

## [7.38.3] - 2024-04-25
### Improved
- The classes `WorkflowUpsert`, `Filter`, `Query`, `Node`, `Edge`, `Container`, `Document`, and
  `Transformation` which are used for parsing API responses were not handling adding new parameters in
  the API correctly. These are now future-proofed.

## [7.38.2] - 2024-04-24
### Added
- Added new parameter `function_external_id` to `FunctionScheduleAPI.create` as a convenience to the user. Note
  that schedules must be attached to a Function by (internal) ID, so a lookup is first done on behalf of the user.

## [7.38.1] - 2024-04-23
### Added
- Added missing `partitions` parameter to `list()` and `__call__()` methods for `FilesAPI`.

## [7.38.0] - 2024-04-22
### Added
- Support for `workflows.executions.retry`

## [7.37.4] - 2024-04-22
### Improved
- Enabled automatic retries on Data Workflows POST endpoints

## [7.37.3] - 2024-04-18
### Improved
- Minor quality of life change for comparing capabilities involving `DataModelInstancesAcl.WRITE_PROPERTIES`; any
  ACL already covered by `WRITE` will not be reported as missing.

## [7.37.2] - 2024-04-18
### Fixed
- Datapoints inserted into non-existent time series, no longer get their identifier hidden in the `failed` attribute
  on the raised `CogniteNotFoundError`. Any `successful` now also gets reported correctly.

## [7.37.1] - 2024-04-17
### Fixed
- Updating data set ID now works as expected for `ThreeDModelUpdate`.

## [7.37.0] - 2024-04-16
### Fixed
- Now handle unknown data types in DM

## [7.36.0] - 2024-04-16
### Fixed
- Now handle unknown filter types in DM
- Add support for the "invalid" filter type in DM

## [7.35.0] - 2024-04-16
### Added
- Datapoints insert methods `insert` and `insert_multiple` now support ingesting (optional) status codes.

## [7.34.0] - 2024-04-11
### Added
- Datapoints method `retrieve_latest` now supports status codes.
- Slicing or indexing a `Datapoints` or `DatapointsArray` instance, now propagates status codes (when present).

## [7.33.1] - 2024-04-10
### Fixed
- Ordering of elements from calls to `retrieve_multiple` now match the requested elements. For SDK versions between
  7.0.0 and 7.33.1, the ordering has been broken when >> 1k elements has been requested (the more requests used, the
  more likely that a chunk was out of order).

## [7.33.0] - 2024-04-08
### Added
- All datapoints retrieve methods (except `retrieve_latest`) now support status codes. Note: Support for *inserting*
  datapoints with status codes will be released shortly. There are three new arguments:
    * `include_status (bool)`: Toggle the return of status code and -symbol on/off, only valid for raw datapoints.
    * `ignore_bad_datapoints (bool)`: For raw datapoints: Whether to return those marked bad (or not).
      For aggregates: Whether the time periods of bad datapoints should affect aggregate calculations (or not).
    * `treat_uncertain_as_bad (bool)`: Toggle whether datapoints marked uncertain should be regarded as good or bad.
- The `to_pandas` method for `Datapoints`, `DatapointsList`, `DatapointsArray` and `DatapointsArrayList` now accepts
  a new parameter, `include_status (bool)`, that controls whether to include status codes & -symbols as separate columns.
- New datapoints query class, `DatapointsQuery`, to make writing custom queries easier, type-safe and more robust,
  as opposed to passing dictionaries (of settings).
### Deprecated
- Passing *custom* datapoints queries using dictionaries is deprecated and will be removed in the next major release.
  Consider refactoring already to `DatapointsQuery`. Example: `{"id": 12, "aggregates" : "min", "granularity": "6h"} ->
  DatapointsQuery(id=12, aggregates="min", granularity="6h")`.

## [7.32.8] - 2024-04-08
### Fixed
- When using TimeSeries objects without `external_id` as part of the `variables` parameter in a synthetic datapoints
  query, a `CogniteNotFoundError` would most likely be raised, due to `None` being silently cast to a string. It now
  raises a friendly `ValueError`.
- An invalid expression could be created when using multiple variables in a synthetic datapoints query. This happened
  while substituting the variables into the expression; this was done one at a time, leading to later replacements
  possibly affecting earlier ones. Now all variables are substituted at the same time/in a single call.
### Improved
- Passing sympy symbols as part of the variables mapping (in synthetic datapoints queries) is now documented properly
  and "officially supported".

## [7.32.7] - 2024-04-05
### Fixed
- Inserting sequence data using `insert_dataframe` would by default drop all rows that contained at least one missing value.
  This has now been fixed to only remove rows where all values are missing.

## [7.32.6] - 2024-04-05
### Fixed
- `AssetsAPI.create_hierarchy` now properly supports `AssetWrite`.

## [7.32.5] - 2024-04-04
### Improved
- Type validation of identifiers

## [7.32.4] - 2024-03-28
### Fixed
- Several methods for `DatapointsArray` that previously failed for string datapoints due to bad handling
  of numpy `dtype`-to-native conversion.

## [7.32.3] - 2024-03-27
### Removed
- Support for `protobuf==3.*` was dropped.

## [7.32.2] - 2024-03-26
### Added
- Missing filterable properties `unit_external_id` and `unit_quantity` to `DatapointSubscriptionProperty`.
  Note: was renamed from `DatapointSubscriptionFilterProperties`, which is now a deprecated alias.

## [7.32.1] - 2024-03-25
### Fixed
- Fix type hints for functions data classes Function/FunctionSchedule/FunctionCall

## [7.32.0] - 2024-03-25
### Changed
- Type hint for `id`, `last_updated_time`, and `create_time` attributes are no longer `Optional` on
  subclasses of `CogniteResource`. This is to reflect that these attributes are always set when the
  object is returned by the SDK.

## [7.31.0] - 2024-03-24
### Added
- Retrieve method for session, `client.iam.session.retrieve`
- The parameter `limit` to the method `client.iam.session.list`.
### Fixed
- The method `client.iam.session.revoke` is now overloaded correctly and returns a `Session` for single id
  and a `SessionList` for multiple ids.

## [7.30.1] - 2024-03-23
### Fixed
- When calling `client.sequences.data.retrieve` in a Jupyter Notebook the returning `SequenceRowsList` no longer raises
  `AttributeError: 'dict' object has no attribute '_repr_html_'` (the HTML representation of `SequenceRowsList` was failing).

## [7.30.0] - 2024-03-20
### Added
- `Properties` class, as used on e.g. `Node` and `Edge`, now renders in Jupyter Notebooks (`_repr_html_` added).

## [7.29.0] - 2024-03-20
### Added
- Direct access to the columns/data stored on raw rows have been added (alongside a `.get` method). Example usage:
  `row["my_col"]` (short-cut for: `row.columns["my_col"]`).

## [7.28.2] - 2024-03-14
### Fixed
- Retrieving more than 100 containers, views, data models, or spaces no longer raises a `CogniteAPIError`.

## [7.28.1] - 2024-03-13
### Fixed
- Fixed issue causing multipart file upload to fail when mime-type was set.

## [7.28.0] - 2024-03-13
### Added
- Added support for advanced filter query in the `list()` (and `__call__()`) method of `assets`, `events`, `sequences`,
  and `time_series` APIs. Now you are able to use advanced filter (like in `filter()`) at the same time as the simple
  filter properties, allowing for more complex requests.
- Added missing `sort` parameter to `list()` and `__call__()` methods for `AssetsAPI`.
- Added missing `sort` parameter to `list()` and `__call__()` methods for `TimeSeriesAPI`.
- Added missing `sort` and `partitions` parameters to `list()` and `__call__()` methods for `SequencesAPI`.
### Deprecated
- Added a deprecation warning on the `filter()` method of `assets`, `events`, `sequences`, and `time_series` APIs as
  its functionality is fully covered by the `list()` method.

## [7.27.2] - 2024-03-08
### Added
- Retry 429s on graphql endpoints

## [7.27.1] - 2024-03-08
### Improved
- When iterating raw rows concurrently, a max queue size for pending results have been added to keep a stable low
  bounded memory usage profile (for when the caller's code isn't processing fast enough to keep up). Worth noting
  that this has no effect on the total retrieval time.

## [7.27.0] - 2024-03-04
### Added
- Added support for multipart file uploads using the `client.files.multipart_upload_session` method.

## [7.26.2] - 2024-03-05
### Fixed
- Fixed a regression from 7.26.1 in the logic for when to refresh token.

## [7.26.1] - 2024-03-05
### Fixed
- The `CredentialProvider` class for client credentials, `OAuthClientCredentials`, was switched from using the non-standard
  field `expires_at` to `expires_in` that's part of the OAuth 2.0 standard (RFC 6749).

## [7.26.0] - 2024-02-29
### Added
- In data modeling, added support for setting floats with units in containers. In addition, added support for retrieving,
  listing, searching, aggregating, querying and syncing nodes/edges with a target unit or target unit system.

## [7.25.0] - 2024-02-29
### Added
- Support for sorting on `client.data_modeling.instances.search`

## [7.24.4] - 2024-02-28
### Fixed
- Unknown ACLs, actions or scopes no longer causes `IAMAPI.[groups.list(...), token.inspect()]` to raise.
### Added
- New action for `DataModelInstancesAcl` added: `Write_Properties`.

## [7.24.3] - 2024-02-28
### Fixed
- Fix handling of GeometryCollection objects in the Documents API.

## [7.24.2] - 2024-02-25
### Fixed
- [Pyodide/WASM only] The list method for raw rows now works for non-finite queries (got broken in `7.24.1`).

## [7.24.1] - 2024-02-25
### Fixed
- [Pyodide/WASM only] The iteration method for raw rows now yields rows _while running_ (instead of waiting for tasks to finish first).

## [7.24.0] - 2024-02-25
### Added
- New parameter for `client.raw.rows(...)`: `partitions`. This enables greater throughput thorough concurrent reads when using
  the generator method (while still keeping a low memory impact). For backwards compatibility, the default is _no concurrency_.
  When specified, can be used together with a finite limit, as opposed to most (if not all) other resources/APIs.
- New parameter for `client.raw.rows.list(...)`: `partitions`. For backwards compatibility, the default is _no concurrency_ when
  a finite `limit` is given, and _"max" concurrency_ (`partitions=max_workers`) otherwise. Partitions can be used with finite limits.
  With this change it is easy to set an appropriate level of concurrency without messing with the global client configuration.
### Changed
- Default configuration setting of `max_workers` has been changed from 10 to 5 (to match the documentation).

## [7.23.1] - 2024-02-23
### Fixed
- Add missing `partition` scope to `seismicAcl`.

## [7.23.0] - 2024-02-23
### Added
- Make properties on instances (`Node`, `Edge`) easier to work with, by implementing support for direct indexing (and a `.get` method).
  If the instances have properties from no source or multiple sources, an error is raised instead. Example usage: `instance["my_prop"]`
  (short-cut for: `instance.properties[ViewId("space", "ext.id", "version")]["my_prop"]`)

## [7.22.0] - 2024-02-21
### Added
- Data point subscriptions reaches General Availability (GA).
  - Use the new [Data point subscriptions](https://developer.cognite.com/dev/concepts/data_point_subscriptions/)
    feature to configure a subscription to listen to changes in one or more time series (in ingestion order).
    The feature is intended to be used where data points consumers need to keep up to date with
    changes to one or more time series without the need to read the entire time series again.
### Changed
- Removed the `ignore_unknown_ids` flag from `client.time_series.subscriptions.retrieve()` to stay consistent with other resource types.

## [7.21.1] - 2024-02-20
### Fixed
- Data Workflows: mark parameter `jobId` as optional in `TransformationTaskOutput`, as it may not be populated in case of a failure.

## [7.21.0] - 2024-02-10
### Added
- Parameter `sort` to `client.documents.list`.

## [7.20.1] - 2024-02-19
### Fixed
- `DMLApplyResult` no longer fails when converted to a string (representation).

## [7.20.0] - 2024-02-13
### Fixed
- internal json encoder now understands CogniteObject and CogniteFilter objects, so that they are
  correctly serialized when used in nested structures.

## [7.19.2] - 2024-02-13
### Fixed
- Addressed `FutureWarning` coming from pandas dependency (granularity to pandas frequency translation of sec/min/hour and 'year start')
- Fixed `granularity` setting in `DatapointsAPI.retrieve_dataframe_in_tz` showing up as number of hours instead of e.g. week or year.

## [7.19.1] - 2024-02-12
### Fixed
- Calls to ... are now retried automatically:
    * Functions API: `list`, `retrieve`, `retrieve_multiple`, `activate`
    * FunctionCalls API: `list`, `retrieve`
    * FunctionSchedules API: `list`, `retrieve`
    * ExtractionPipelines API: `retrieve_multiple`
    * ExtractionPipelineRuns API: `list`
    * Transformations API: `list`, `retrieve`, `retrieve_multiple`, `preview`
    * TransformationJobs API: `retrieve`, `retrieve_multiple`
    * TransformationSchedules API: `retrieve`, `retrieve_multiple`
    * Geospatial API:  `list_feature_types`, `retrieve_feature_types`, `retrieve_features`, `list_features`,
      `search_features`, `stream_features`, `aggregate_features`, `get_coordinate_reference_systems`, `get_raster`, `compute`,
    * UserProfiles API: `retrieve`, `search`
    * Documents API: `search`, `list`, `__call__`, `aggregate_count`, `aggregate_cardinality_values`, `aggregate_cardinality_properties`,
      `aggregate_unique_values`, `aggregate_unique_properties`
    * ThreeDRevisions API: `filter_nodes`

## [7.19.0] - 2024-02-12
### Added
- Helper methods to `View`, `ViewApply`, `ViewList` and `ViewApplyList` `referenced_containers` which returns the
  containers referenced by in the view(s).

## [7.18.0] - 2024-02-08
### Added
- Support for `target_unit` and `target_unit_system` in synthetic time series.

## [7.17.4] - 2024-02-07
### Added
- Allow using container property reference in `NodeResultSetExpression.through` in addition to view property reference

## [7.17.3] - 2024-02-06
### Fixed
- Creating a Cognite Function from a directory with `skip_folder_validation=False` no longer raises `ModuleNotFoundError`
  for Pyodide (WASM) users.

## [7.17.2] - 2024-02-04
### Fixed
- Uploading files now accepts Labels again as part of file metadata. This addresses a bug introduced in v7, which caused
  a `ValueError` to be raised.

## [7.17.1] - 2024-02-02
### Fixed
- An (extreme) edge case where an empty, unnecessary API request for datapoints would be sent leading to a `CogniteAPIError`.
- Certain granularity inputs (when using the `DatapointsAPI`) no longer cause a `ValueError` to be raised with confusing/wrong wording.

## [7.17.0] - 2024-02-01
### Fixed
- Calls to `AnnotationsAPI.[list|retrieve|retrieve_multiple|reverse_lookup]` are now retried automatically.
- Calls to `AnnotationsAPI.reverse_lookup` now also accept the standard values (`-1, inf`) to indicate 'no limit'.
### Improved
- Calls to `AnnotationsAPI.list` with more than 1000 `annotated_resource_ids` are now batched automatically for the user.
  Previously these would raise an API error.

## [7.16.0] - 2024-01-30
### Added
- When listing instances (and when using `search`, `aggregate` and `histogram`), a new `space` parameter has been added;
  you may pass either a single space identifier (or a list of several). Note that this is just for convenience, using
  `filter` still works (and is necessary for more complex queries).
- New convenience filter, `SpaceFilter`, makes filtering on space simpler.

## [7.15.1] - 2024-01-23
### Fixed
- When calling `to_pandas` with `expand_properties=True` on an instance or instance list with no properties, the SDK will
  no longer raise ValueError, but drop the empty properties row/column.

## [7.15.0] - 2024-01-22
### Improved
- Only run pypi version check once, despite instantiating multiple clients. And make it async too.

## [7.14.0] - 2024-01-22
### Changed
- Helper methods to get related resources on `Asset` class now accept `asset_ids` as part of keyword arguments.
### Added
- Helper methods to get related resources on `AssetList` class now accept keyword arguments that are passed on to
  the list endpoint (for server-side filtering).

## [7.13.8] - 2024-01-19
### Fixed
- `FilesAPI.upload` when using `geo_location` (serialize error).

## [7.13.7] - 2024-01-19
### Fixed
- Type hints for all `.update` and `.upsert` methods accept Write classes in addition to Read and Update classes.
- Missing overloading of the `.update` methods on `client.three_d.models.update`, `client.transformations.update`,
  `client.transformations.schedules.update`, `client.relationships.update`, and `client.data_sets.update`.

## [7.13.6] - 2024-01-18
### Added
- Helper method `as_tuple` to `NodeId` and `EdgeId`.

## [7.13.5] - 2024-01-16
### Added
- EdgeConnection, MultiEdgeConnection, MultiReverseDirectRelation and their corresponding Apply View dataclasses are now importable from `cognite.client.dataclasses.data_modeling`.

## [7.13.4] - 2024-01-11
### Fixed
- When calling `WorkflowExecution.load` not having a `schedule` would raise a `KeyError` even though it is optional. This is now fixed.
- When calling `Datapoints.load` not having a `isString` would raise a `KeyError` even though it is optional. This is now fixed.
- Most `CogniteResourceList.as_write()` would raise a `CogniteMissingClientError` when called from a class with missing cognite_client. This is now fixed.

## [7.13.3] - 2024-01-12
### Added
- `View.as_property_ref` and `Container.as_property_ref` to make it easier to create property references
  (used to only be available on `ViewId` and `ContainerId`).

## [7.13.2] - 2024-01-11
### Fixed
- When calling `ExtractionPipeline.load` not having a `schedule` would raise a `KeyError` even though it is optional. This is now fixed.

## [7.13.1] - 2024-01-10
### Improved
- Respect the `isAutoRetryable` flag on error responses from the API when retrying requests.

## [7.13.0] - 2024-01-09
### Changed
- Units on Time Series (including unit conversion) is out of beta and will no longer issue warnings on usage.

## [7.12.0] - 2024-01-09
### Added
- `DatapointsAPI.retrieve_latest` now accepts `target_unit` or `target_unit_system` parameter.
### Fixed
- `DatapointsAPI.retrieve_latest` when given `LatestDatapointQuery`(s) without a setting for `before`, now correctly use
  the (default) `before` setting as specified in the method call.

## [7.11.0] - 2024-01-09
### Added
- All Cognite resources now have write-version. For example, we have `Asset` and `AssetWrite`, `Event` and `EventWrite`, and so on.
  The new write class reflects the required/optional fields in the API, and is now recommended when creating resources. In addition,
  all read classes and list classes now have a convenience method `as_write` that returns the write class with the same data.
  For example, if you have a `assets` of type `AssetList` you can call `assets.as_write()` which will return a `AssetWriteList`,
  and thus removing all server set fields (like `created_time` and `last_updated_time`). This is useful if you want to
  compare a resource from CDF with a local configuration. In addition, this makes it easier to create a new resource
  using an existing resource as a template.
- Missing overloading of the `.create` methods on `client.iam.security_categories.create`, `client.iam.groups.create`,
  `client.labels.create`, `client.three_d.models.create`, `client.three_d.revisions.create`, `client.three_d.asset_mappings.create`,
  `client.transformations.create`, `client.transformations.schedules.create`, and `client.relationships.create`.
### Changed
- The class `DatapointSubscriptionCreate` has been renamed to `DatapointSubscriptionWrite` to be consistent with the other write classes.
  This is not a breaking change, as the old class is still available for backwards compatibility, but will be removed in the next major version.
### Fixed
- The `node.type` was not set when calling `.as_apply()` or `.as_write()` on a `Node` or `NodeList`. This is now fixed.

## [7.10.1] - 2024-01-08
### Added
- Fix retries for `POST /raw/rows`.

## [7.10.0] - 2024-01-08
### Added
- `geospatial.search_features` and `geospatial.stream_features` now accept the `allow_dimensionality_mismatch` parameter.

## [7.9.0] - 2024-01-05
### Added
- You can now enable or disable user profiles for your CDF project with `client.iam.user_profiles.[enable/disable]`.

## [7.8.10] - 2024-01-04
### Changed
- When using `OidcCredentials` to create a transformation, `cdf_project_name` is no longer optional as required
  by the API.

## [7.8.9] - 2024-01-04
### Fixed
- Pyodide-users of the SDK can now create Transformations with non-nonce credentials without a `pyodide.JsException`
  exception being raised.

## [7.8.8] - 2024-01-03
### Added
- Support for `workflows.cancel`.

## [7.8.7] - 2024-01-03
### Fixed
- Added back `InstancesApply` that was removed in 7.8.6.

## [7.8.6] - 2023-12-27
### Improved
- SDK dependency on the `sortedcontainers` package was dropped.

## [7.8.5] - 2023-12-22
### Fixed
- `DirectRelationReference` is now immutable.
- `DirectRelationReference.load` now correctly handles unknown parameters.

## [7.8.4] - 2023-12-22
### Fixed
- Listing annotations now also accepts `None` and `inf` for the `limit` parameter (to return all), matching what
  was already described in the documentation for the endpoint (for the parameter).
- Calling `to_pandas(...)` on an `DiagramDetectItem` no longer raises `KeyError`.

## [7.8.3] - 2023-12-21
### Fixed
- Revert `SingleHopConnectionDefinition` from a string to child class of `ViewProperty`.
- If a `ViewProperty` or `ViewPropertyApply` dumped before version `7.6` was dumped and loaded after `7.6`, the
  user got a `KeyError: 'container'`. The `load` methods are now backwards compatible with the old format.

## [7.8.2] - 2023-12-21
### Fixed
- Revert `SingleHopConnectionDefinitionApply` from a string to child class of `ViewPropertyApply`.

## [7.8.1] - 2023-12-21
### Fixed
- Calling `to_pandas` with `expand_aggregates=True` on an Asset with aggregated properties would yield a pandas DataFrame
  with the column name `0` instead of `"value"`.
### Improved
- Specification of aggregated properties to `AssetsAPI.[list,filter,__call__]`.

## [7.8.0] - 2023-12-21
### Added
- Instance classes `Node`, `Edge`, `NodeList` and `EdgeList` now supports a new flag `expand_properties` in their `to_pandas` method,
  that makes it much simpler to work with the fetched properties. Additionally, `remove_property_prefix` allows easy prefix
  removal (of the view ID, e.g. `space.external_id/version.my_prop` -> `my_prop`).

## [7.7.1] - 2023-12-20
### Fixed
- Missing legacy capability ACLs: `modelHostingAcl` and `genericsAcl`.
- The `IAMAPI.compare_capabilities` fails with a `AttributeError: 'UnknownAcl' object has no attribute '_capability_name'`
  if the user has an unknwon ACL. This is now fixed by skipping comparison of unknown ACLs and issuing a warning.

## [7.7.0] - 2023-12-20
### Added
- Support for `ViewProperty` types `SingleReverseDirectRelation` and `MultiReverseDirectRelation` in data modeling.

## [7.6.0] - 2023-12-13
### Added
- Support for querying data models through graphql. See `client.data_modeling.graphql.query`.

## [7.5.7] - 2023-12-12
### Fixed
- Certain combinations of `start`/`end` and `granularity` would cause `retrieve_dataframe_in_tz` to raise due to
  a bug in the calender-arithmetic (`MonthAligner`).

## [7.5.6] - 2023-12-11
### Added
- Missing legacy scopes for `Capability`: `LegacySpaceScope` and `LegacyDataModelScope`.

## [7.5.5] - 2023-12-11
### Added
- Added `poll_timeout` parameter on `time_series.subscriptions.iterate_data`. Will keep the connection open and waiting,
  until new data is available, up to `poll_timeout` seconds.

## [7.5.4] - 2023-12-06
### Changed
- The `partitions` parameter is no longer respected when using generator methods to list resources
- The `max_workers` config option has been moved from ClientConfig to the global config.

## [7.5.3] - 2023-12-06
### Added
- Support for `subworkflow` tasks in `workflows`.

## [7.5.2] - 2023-12-05
### Fixed
- The built-in `hash` function was mistakenly stored on `WorkflowDefinitionUpsert` instances after `__init__` and has been removed.

## [7.5.1] - 2023-12-01
### Changed
- Raise an exception if `ClientConfig:base_url` is set to `None` or an empty string

## [7.5.0] - 2023-11-30
### Added
- `chain_to` to `NodeResultSetExpression` and `NodeResultSetExpression`, and `direction` to `NodeResultSetExpression`.

## [7.4.2] - 2023-11-28
### Improved
- Quality of life improvement to `client.extraction_pipelines.runs.list` method. The `statuses` parameter now accepts
  a single value and the annotation is improved. The parameter `created_time` can now be given on the format `12d-ago`.

## [7.4.1] - 2023-11-28
### Fixed
- Error in validation logic when creating a `Transformation` caused many calls to `client.transformations.update` to fail.

## [7.4.0] - 2023-11-27
### Changed
- Unit Catalog API is out of beta and will no longer issue warnings on usage. Access is unchanged: `client.units`.

## [7.3.3] - 2023-11-22
### Fixed
- Added action `Delete` in `ProjectsAcl`.

## [7.3.2] - 2023-11-21
### Fixed
- `workflows.retrieve` and `workflows.versions.retrieve` returned None if the provided workflow external id contained special characters. This is now fixed.

## [7.3.1] - 2023-11-21
### Fixed
- Replaced action `Write` with `Create` in `ProjectsAcl`, as `Write` is not a valid action and `Create` is the correct one.

## [7.3.0] - 2023-11-20
### Added
- Added Scope `DataSet` for `TimeSeriesSubscriptionsAcl`.
- Added `data_set_id` to `DatapointSubscription`.

## [7.2.1] - 2023-11-17
### Fixed
- The new compare methods for capabilities in major version 7, `IAMAPI.verify_capabilities` and `IAMAPI.compare_capabilities`
  now works correctly for rawAcl with database scope ("all tables").
### Removed
- Capability scopes no longer have the `is_within` method, and capabilities no longer have `has_capability`. Use the more
  general `IAMAPI.compare_capabilities` instead.

## [7.2.0] - 2023-11-16
### Added
- The `trigger` method of the Workflow Execution API, now accepts a `client_credentials` to allow specifying specific
  credentials to run with. Previously, the current credentials set on the CogniteClient object doing the call would be used.

## [7.1.0] - 2023-11-16
### Added
- The list method for asset mappings in the 3D API now supports `intersects_bounding_box`, allowing users to only
  return asset mappings for assets whose bounding box intersects with the given bounding box.

## [7.0.3] - 2023-11-15
### Fixed
- Bug when `cognite.client.data_classes.filter` used with any `data_modeling` endpoint raised a `CogniteAPIError` for
  snake_cased properties. This is now fixed.
- When calling `client.relationships.retrieve`, `.retrieve_multiple`, or `.list` with `fetch_resources=True`, the
  `target` and `source` resources were not instantiated with a `cognite_client`. This is now fixed.

## [7.0.2] - 2023-11-15
### Fixed
- Missing Scope `DataSet` for `TemplateGroupAcl` and `TemplateInstancesAcl`.

## [7.0.1] - 2023-11-14
### Fixed
- Data modeling APIs now work in WASM-like environments missing the threading module.

## [7.0.0] - 2023-11-14
This release ensure that all CogniteResources have `.dump` and `.load` methods, and that calling these two methods
in sequence produces an equal object to the original, for example,
`my_asset == Asset.load(my_asset.dump(camel_case=True)`. In addition, this ensures that the output of all `.dump`
methods are `json` and `yaml` serializable. Additionally, the default for `camel_case` has been changed to `True`.

### Improved
- Read operations, like `retrieve_multiple` will now fast-fail. Previously, all requests would be executed
  before the error was raised, potentially fetching thousands of unneccesary resources.

### Added
- `CogniteResource.to_pandas` and `CogniteResourceList.to_pandas` now converts known timestamps to `datetime` by
  default. Can be turned off with the new parameter `convert_timestamps`. Note: To comply with older pandas v1, the
  dtype will always be `datetime64[ns]`, although in v2 this could have been `datetime64[ms]`.
- `CogniteImportError` can now be caught as `ImportError`.

### Deprecated
- The Templates API (migrate to Data Modeling).
- The `client.assets.aggregate` use `client.assets.aggregate_count` instead.
- The `client.events.aggregate` use `client.events.aggregate_count` instead.
- The `client.sequence.aggregate` use `client.sequence.aggregate_count` instead.
- The `client.time_series.aggregate` use `client.time_series.aggregate_count` instead.
- In `Transformations` attributes `has_source_oidc_credentials` and `has_destination_oidc_credentials` are deprecated,
  and replaced by properties with the same names.

### Changed
- All `.dump` methods now uses `camel_case=True` by default. This is to match the intended use case, preparing the
  object to be sent in an API request.
- `CogniteResource.to_pandas` now more closely resembles `CogniteResourceList.to_pandas` with parameters
`expand_metadata` and `metadata_prefix`, instead of accepting a sequence of column names (`expand`) to expand,
with no easy way to add a prefix. Also, it no longer expands metadata by default.
- Additionally, `Asset.to_pandas`, now accepts the parameters `expand_aggregates` and `aggregates_prefix`. Since
  the possible `aggregates` keys are known, `camel_case` will also apply to these (if expanded) as opposed to
  the metadata keys.
- More narrow exception types like `CogniteNotFoundError` and `CogniteDuplicatedError` are now raised instead of
  `CogniteAPIError` for the following methods: `DatapointsAPI.retrieve_latest`, `RawRowsAPI.list`,
  `RelationshipsAPI.list`, `SequencesDataAPI.retrieve`, `SyntheticDatapointsAPI.query`. Additionally, all calls
  using `partitions` to API methods like `list` (or the generator version) now do the same.
- The `CogniteResource._load` has been made public, i.e., it is now `CogniteResource.load`.
- The `CogniteResourceList._load` has been made public, i.e., it is now `CogniteResourceList.load`.
- All `.delete` and `.retrieve_multiple` methods now accepts an empty sequence, and will return an empty `CogniteResourceList`.
- All `assert`s meant for the SDK user, now raise appropriate errors instead (`ValueError`, `RuntimeError`...).
- `CogniteAssetHierarchyError` is no longer possible to catch as an `AssertionError`.
- Several methods in the data modelling APIs have had parameter names now correctly reflect whether they accept
  a single or multiple items (i.e. id -> ids).
- `client.data_modeling.instances.aggregate` returns `AggregatedNumberedValue | list[AggregatedNumberedValue] | InstanceAggregationResultList` depending
  on the `aggregates` and `group_by` parameters. Previously, it always returned `InstanceAggregationResultList`.
- The `Group` attribute `capabilities` is now a `Capabilities` object, instead of a `dict`.
- Support for `YAML` in all `CogniteResource.load()` and `CogniteResourceList.load()` methods.
- The `client.sequences.data` methods `.retrieve`, `.retrieve_last_row` (previously `retrieve_latest`), `.insert`  method has changed signature:
  The parameter `column_external_ids` is renamed `columns`. The old parameter `column_external_ids` is still there, but is
  deprecated. In addition, int the `.retrieve` method, the parameters `id` and `external_id` have
  been moved to the beginning of the signature. This is to better match the API and have a consistent overload
  implementation.
- The class `SequenceData` has been replaced by `SequenceRows`. The old `SequenceData` class is still available for
  backwards compatibility, but will be removed in the next major version. However, all API methods now return
  `SequenceRows` instead of `SequenceData`.
- The attribute `columns` in `Sequence` has been changed from `typing.Sequence[dict]` to `SequnceColumnList`.
- The class `SequenceRows` in `client.data_classes.transformations.common` has been renamed to `SequenceRowsDestination`.
- The `client.sequences.data.retrieve_latest` is renamed `client.sequences.data.retrieve_last_row`.
- Classes `Geometry`, `AssetAggregate`, `AggregateResultItem`, `EndTimeFilter`, `Label`, `LabelFilter`, `ExtractionPipelineContact`,
  `TimestampRange`, `AggregateResult`, `GeometryFilter`, `GeoLocation`, `RevisionCameraProperties`, `BoundingBox3D` are no longer
  `dict` but classes with attributes matching the API.
- Calling `client.iam.token.inspect()` now gives an object `TokenInspection` with attribute `capabilities` of type `ProjectCapabilityList`
  instead of `list[dict]`
- In data class `Transformation` the attribute `schedule`, `running_job`, and `last_running_job`, `external_id` and `id`
  are set to the `Transformation` `id` and `external_id` if not set. If they are set to a different value, a `ValueError` is raised

### Added
- Added `load` implementation for `VisionResource`s: `ObjectDetection`, `TextRegion`, `AssetLink`, `BoundingBox`,
  `CdfRerourceRef`, `Polygon`, `Polyline`, `VisionExtractPredictions`, `FeatureParameters`.
- Missing `dump` and `load` methods for `ClientCredentials`.
- Literal annotation for `source_type` and `target_type` in `Relationship`
- In transformations, `NonceCredentials` was missing `load` method.
- In transformations, `TransformationBlockedInfo` was missing `.dump` method
- `capabilities` in `cognite.client.data_classes` with data classes for all CDF capabilities.
- All `CogniteResource` and `CogniteResourcelist` objects have `.dump_yaml` methods, for example, `my_asset_list.dump_yaml()`.

### Removed
- Deprecated methods `aggregate_metadata_keys` and `aggregate_metadata_values` on AssetsAPI.
- Deprecated method `update_feature_types` on GeospatialAPI.
- Parameters `property` and `aggregates` for method `aggregate_unique_values` on GeospatialAPI.
- Parameter `fields` for method `aggregate_unique_values` on EventsAPI.
- Parameter `function_external_id` for method `create` on FunctionSchedulesAPI (function_id has been required
  since the deprecation of API keys).
- The `SequenceColumns` no longer set the `external_id` to `column{no}` if it is missing. It now must be set
  explicitly by the user.
- Dataclasses `ViewDirectRelation` and `ContainerDirectRelation` are replaced by `DirectRelation`.
- Dataclasses `MappedPropertyDefinition` and `MappedApplyPropertyDefinition` are replaced by `MappedProperty` and `MappedPropertyApply`.
- Dataclasses `RequiresConstraintDefinition` and `UniquenessConstraintDefinition` are replaced by `RequiresConstraint` and `UniquenessConstraint`.
- In data class `Transformation` attributes `has_source_oidc_credentials` and `has_destination_oidc_credentials` are replaced by properties.

### Fixed
- Passing `limit=0` no longer returns `DEFAULT_LIMIT_READ` (25) resources, but raises a `ValueError`.
- `Asset.dump()` was not dumping attributes `geo_location` and `aggregates` to `json` serializable data structures.
- In data modeling, `NodeOrEdgeData.load` method was not loading the `source` attribute to `ContainerId` or `ViewId`. This is now fixed.
- In data modeling, the attribute `property` used in `Node` and `Edge` was not `yaml` serializable.
- In `DatapointsArray`, `load` method was not compatible with `.dump` method.
- In extraction pipelines, `ExtractionPipelineContact.dump` was not `yaml` serializable
- `ExtractionPipeline.dump` attribute `contacts` was not `json` serializable.
- `FileMetadata.dump` attributes `labels` and `geo_location` was not `json` serializable.
- In filtering, filter `ContainsAll` was missing in `Filter.load` method.
- Annotation for `cpu` and `memory` in `Function`.
- `GeospatialComputedResponse.dump` attribute `items` was not `yaml` serializable
- `Relationship.dump` was not `json` serializable.
- `Geometry.dump` was not `json` serializable.
- In templates, `GraphQlResponse.dump` was not `json` serializable, and `GraphQlResponse.dump` failed to load
  `errors` `GraphQlError`.
- `ThreeDModelRevision` attribute `camera` was not dumped as `yaml` serializable and
  not loaded as `RevisionCameraProperties`.
- `ThreeDNode` attribute `bounding_box` was not dumped as `yaml` serializable and
  not loaded as `BoundingBox3D`.
- `Transformation` attributes `source_nonce`, `source_oidc_credential`, `destination_nonce`,
  and `destination_oidc_credentials` were not dumped as `json` serializable and `loaded` with
  the appropriate data structure. In addition, `TransformationBlockedInfo` and `TransformationJob`
  were not dumped as `json` serializable.
- `TransformationPreviewResult` was not dumping attribute `schema` as `yaml` serializable, and the
  `load` and `dump` methods were not compatible.
- In transformations, `TransformationJob.dump` was not `json` serializable, and attributes
  `destination` and `status` were not loaded into appropriate data structures.
- In transformations, `TransformationSchemaMapType.dump` was not `json` serializable.
- In `annotation_types_images`, implemented `.load` for `KeypointCollection` and `KeypointCollectionWithObjectDetection`.
- Bug when dumping `documents.SourceFile.dump(camel_case=True)`.
- Bug in `WorkflowExecution.dump`
- Bug in `PropertyType.load`

## [6.39.6] - 2023-11-13
## Fixed
- HTTP status code retry strategy for RAW and labels. `/rows/insert` and `/rows/delete` will now
  be retried for all status codes in `config.status_forcelist` (default 429, 502, 503, 504), while
  `/dbs/{db}` and `/tables/{table}` will now only be retried for 429s and connection errors as those
  endpoints are not idempotent.
- Also, `labels/list` will now also be retried.

## [6.39.5] - 2023-11-12
## Fixed
- The `.apply()` methods of `MappedProperty` now has the missing property `source`.

## [6.39.4] - 2023-11-09
## Fixed
- Fetching datapoints from dense time series using a `targetUnit` or a target `targetUnitSystem` could result
  in some batches not being converted to the new unit.

## [6.39.3] - 2023-11-08
## Fixed
- The newly introduced parameter `connectionType` was assumed to be required from the API. This is not the case.

## [6.39.2] - 2023-11-08
## Fixed
- When listing `client.data_modeling.views` the SDK raises a `TypeError`. This is now fixed.

## [6.39.1] - 2023-11-01
## Fixed
- When creating transformations using backup auth. flow (aka a session could not be created for any reason),
  the scopes for the credentials would not be passed correctly (bug introduced in 6.25.1).

## [6.39.0] - 2023-11-01
## Added
- Support for `concurrencyPolicy` property in Workflows `TransformationsWorker`.

## [6.38.1] - 2023-10-31
### Fixed
- `onFailure` property in Workflows was expected as mandatory and was raising KeyError if it was not returned by the API.
  The SDK now assumes the field to be optional and loads it as None instead of raising an error.

## [6.38.0] - 2023-10-30
### Added
- Support `onFailure` property in Workflows, allowing marking Tasks as optional in a Workflow.

## [6.37.0] - 2023-10-27
### Added
- Support for `type` property in `NodeApply` and `Node`.

## [6.36.0] - 2023-10-25
### Added
- Support for listing members of Data Point Subscription, `client.time_series.subscriptions.list_member_time_series()`. Note this is an experimental feature.

## [6.35.0] - 2023-10-25
### Added
- Support for `through` on node result set expressions.

### Fixed
- `unit` on properties in data modeling. This was typed as a string, but it is in fact a direct relation.

## [6.34.2] - 2023-10-23
### Fixed
- Loading a `ContainerApply` from source failed with `KeyError` if `nullable`, `autoIncrement`, or `cursorable` were not set
  in the `ContainerProperty` and `BTreeIndex` classes even though they are optional. This is now fixed.

## [6.34.1] - 2023-10-23
### Added
- Support for setting `data_set_id` and `metadata` in `ThreeDModelsAPI.create`.
- Support for updating `data_set_id` in `ThreeDModelsAPI.update`.

## [6.34.0] - 2023-10-20
### Fixed
- `PropertyType`s no longer fail on instantiation, but warn on missing SDK support for the new property(-ies).

### Added
- `PropertyType`s `Float32`, `Float64`, `Int32`, `Int64` now support `unit`.

## [6.33.3] - 2023-10-18
### Added
- `functions.create()` now accepts a `data_set_id` parameter. Note: This is not for the Cognite function, but for the zipfile containing
  the source code files that is uploaded on the user's behalf (from which the function is then created). Specifying a data set may
  help resolve the error 'Resource not found' (403) that happens when a user is not allowed to create files outside a data set.

## [6.33.2] - 2023-10-16
### Fixed
- When fetching datapoints from "a few time series" (implementation detail), all missing, non-ignorable time series
  are now raised together in a `CogniteNotFoundError` rather than only the first encountered.

### Improved
- Datapoints fetching has a lower peak memory consumption when fetching from multiple time series simultaneously.

## [6.33.1] - 2023-10-14
### Fixed
- `Function.list_schedules()` would return schedules unrelated to the function if the function did not have an external id.

## [6.33.0] - 2023-10-13
### Added
- Support for providing `DirectRelationReference` and `NodeId` as direct relation values when
ingesting node and edge data.

## [6.32.4] - 2023-10-12
### Fixed
- Filters using e.g. metadata keys no longer dumps the key in camel case.

## [6.32.3] - 2023-10-12
### Added
- Ability to toggle the SDK debug logging on/off by setting `config.debug` property on a CogniteClient to True (enable) or False (disable).

## [6.32.2] - 2023-10-10
### Added
- The credentials class used in TransformationsAPI, `OidcCredentials`, now also accepts `scopes` as a list of strings
  (used to be comma separated string only).

## [6.32.1] - 2023-10-10
### Added
- Missing `unit_external_id` and `unit_quantity` fields on `TimeSeriesProperty`.

## [6.32.0] - 2023-10-09
### Fixed
- Ref to openapi doc in Vision extract docstring
- Parameters to Vision models can be given as Python dict (updated doc accordingly).
- Don't throw exception when trying to save empty list of vision extract predictions as annotations. This is to avoid having to wrap this method in try-except for every invocation of the method.

### Added
- Support for new computer vision models in Vision extract service: digital gauge reader, dial gauge reader, level gauge reader and valve state detection.

## [6.31.0] - 2023-10-09
### Added
Support for setting and fetching TimeSeries and Datapoints with "real" units (`unit_external_id`).
- TimeSeries has a new field `unit_external_id`, which can be set when creating or updating it. This ID must refer to a
  valid unit in the UnitCatalog, see `client.units.list` for reference.
- If the `unit_external_id` is set for a TimeSeries, then you may retrieve datapoints from that time series in any compatible
  units. You do this by specifying the `target_unit` (or `target_unit_system`) in a call to any of the datapoints `retrieve`
  methods, `retrieve`, `retrieve_arrays`, `retrieve_dataframe`, or `retrieve_dataframe_in_tz`.

## [6.30.2] - 2023-10-09
### Fixed
- Serialization of `Transformation` or `TransformationList` no longer fails in `json.dumps` due to unhandled composite objects.

## [6.30.1] - 2023-10-06
### Added
- Support for metadata on Workflow executions. Set custom metadata when triggering a workflow (`workflows.executions.trigger()`). The metadata is included in results from `workflows.executions.list()` and `workflows.executions.retrieve_detailed()`.

## [6.30.0] - 2023-10-06
### Added
- Support for the UnitCatalog with the implementation `client.units`.

## [6.29.2] - 2023-10-04
### Fixed
- Calling some of the methods `assets.filter()`, `events.filter()`, `sequences.filter()`, `time_series.filter()` without a `sort` parameter could cause a `CogniteAPIError` with a 400 code. This is now fixed.

## [6.29.1] - 2023-10-04
### Added
- Convenience method `to_text` on the `FunctionCallLog` class which simplifies printing out function call logs.

## [6.29.0] - 2023-10-04
### Added
- Added parameter `resolve_duplicate_file_names` to `client.files.download`.
  This will keep all the files when downloading to local machine, even if they have the same name.

## [6.28.5] - 2023-10-03
### Fixed
- Bugfix for serialization of Workflows' `DynamicTasksParameters` during `workflows.versions.upsert` and `workflows.execution.retrieve_detailed`

## [6.28.4] - 2023-10-03
### Fixed
- Overload data_set/create for improved type safety

## [6.28.3] - 2023-10-03
### Fixed
- When uploading files as strings using `client.files.upload_bytes` the wrong encoding is used on Windows, which is causing
  part of the content to be lost when uploading. This is now fixed.

## [6.28.2] - 2023-10-02
### Fixed
- When cache lookup did not yield a token for `CredentialProvider`s like `OAuthDeviceCode` or `OAuthInteractive`, a
  `TypeError` could be raised instead of initiating their authentication flow.

## [6.28.1] - 2023-09-30
### Improved
- Warning when using alpha/beta features.

## [6.28.0] - 2023-09-26
### Added
- Support for the WorkflowOrchestrationAPI with the implementation `client.workflows`.

## [6.27.0] - 2023-09-13
### Changed
- Reduce concurrency in data modeling client to 1

## [6.26.0] - 2023-09-22
### Added
- Support `partition` and `cursor` parameters on `time_series.subscriptions.iterate_data`
- Include the `cursor` attribute on `DatapointSubscriptionBatch`, which is yielded in every iteration
of `time_series.subscriptions.iterate_data`.

## [6.25.3] - 2023-09-19
### Added
- Support for setting and retrieving `data_set_id` in data class `client.data_classes.ThreeDModel`.

## [6.25.2] - 2023-09-12
### Fixed
- Using the `HasData` filter would raise an API error in CDF.

## [6.25.1] - 2023-09-15
### Fixed
- Using nonce credentials now works as expected for `transformations.[create, update]`. Previously, the attempt to create
  a session would always fail, leading to nonce credentials never being used (full credentials were passed to- and
  stored in the transformations backend service).
- Additionally, the automatic creation of a session no longer fails silently when an `CogniteAuthError` is encountered
  (which happens when the credentials are invalid).
- While processing source- and destination credentials in `client.transformations.[create, update]`, an `AttributeError`
  can no longer be raised (by not specifying project).
### Added
- `TransformationList` now correctly inherits the two (missing) helper methods `as_ids()` and `as_external_ids()`.

## [6.25.0] - 2023-09-14
### Added
- Support for `ignore_unknown_ids` in `client.functions.retrieve_multiple` method.

## [6.24.1] - 2023-09-13
### Fixed
- Bugfix for `AssetsAPI.create_hierarchy` when running in upsert mode: It could skip certain updates above
  the single-request create limit (currently 1000 assets).

## [6.24.0] - 2023-09-12
### Fixed
- Bugfix for `FilesAPI.upload` and `FilesAPI.upload_bytes` not raising an error on file contents upload failure. Now `CogniteFileUploadError` is raised based on upload response.

## [6.23.0] - 2023-09-08
### Added
- Supporting for deleting constraints and indexes on containers.

### Changed
- The abstract class `Index` can no longer be instantiated. Use BTreeIndex or InvertedIndex instead.

## [6.22.0] - 2023-09-08
### Added
- `client.data_modeling.instances.subscribe` which lets you subscribe to a given
data modeling query and receive updates through a provided callback.
- Example on how to use the subscribe method to sync nodes to a local sqlite db.

## [6.21.1] - 2023-09-07
### Fixed
- Concurrent usage of the `CogniteClient` could result in API calls being made with the wrong value for `api_subversion`.

## [6.21.0] - 2023-09-06
### Added
- Supporting pattern mode and extra configuration for diagram detect in beta.

## [6.20.0] - 2023-09-05
### Fixed
- When creating functions with `client.functions.create` using the `folder` argument, a trial-import is executed as part of
  the verification process. This could leave leftover modules still in scope, possibly affecting subsequent calls. This is
  now done in a separate process to guarantee it has no side-effects on the main process.
- For pyodide/WASM users, a backup implementation is used, with an improved cleanup procedure.

### Added
- The import-check in `client.functions.create` (when `folder` is used) can now be disabled by passing
  `skip_folder_validation=True`. Basic validation is still done, now additionally by parsing the AST.

## [6.19.0] - 2023-09-04
## Added
- Now possible to retrieve and update translation and scale of 3D model revisions.

## [6.18.0] - 2023-09-04
### Added
- Added parameter `keep_directory_structure` to `client.files.download` to allow downloading files to a folder structure matching the one in CDF.

### Improved
- Using `client.files.download` will still skip files with the same name when writing to disk, but now a `UserWarning` is raised, specifying which files are affected.

## [6.17.0] - 2023-09-01
### Added
- Support for the UserProfilesAPI with the implementation `client.iam.user_profiles`.

## [6.16.0] - 2023-09-01
### Added
- Support for `ignore_unknown_ids` in `client.relationships.retrieve_multiple` method.

## [6.15.3] - 2023-08-30
### Fixed
- Uploading files using `client.files.upload` now works when running with `pyodide`.

## [6.15.2] - 2023-08-29
### Improved
- Improved error message for `CogniteMissingClientError`. Now includes the type of object missing the `CogniteClient` reference.

## [6.15.1] - 2023-08-29
### Fixed
- Bugfix for `InstanceSort._load` that always raised `TypeError` (now public, `.load`). Also, indirect fix for `Select.load` for non-empty `sort`.

## [6.15.0] - 2023-08-23
### Added
- Support for the DocumentsAPI with the implementation `client.documents`.
- Support for advanced filtering for `Events`, `TimeSeries`, `Assets` and `Sequences`. This is available through the
  `.filter()` method, for example, `client.events.filter`.
- Extended aggregation support for `Events`, `TimeSeries`, `Assets` and `Sequences`. This is available through the five
  methods `.aggregate_count(...)`, `aggregate_cardinality_values(...)`, `aggregate_cardinality_properties(...)`,
  `.aggregate_unique_values(...)`, and `.aggregate_unique_properties(...)`. For example,
  `client.assets.aggregate_count(...)`.
- Added helper methods `as_external_ids` and `as_ids` for `EventList`, `TimeSeriesList`, `AssetList`, `SequenceList`,
  `FileMetaDataList`, `FunctionList`, `ExtractionPipelineList`, and `DataSetList`.

### Deprecated
- Added `DeprecationWarning` to methods `client.assets.aggregate_metadata_keys` and
  `client.assets.aggregate_metadata_values`. The use parameter the `fields` in
  `client.events.aggregate_unique_values` will also lead to a deprecation warning. The reason is that the endpoints
  these methods are using have been deprecated in the CDF API.

## [6.14.2] - 2023-08-22
### Fixed
- All data modeling endpoints will now be retried. This was not the case for POST endpoints.

## [6.14.1] - 2023-08-19
### Fixed
- Passing `sources` as a tuple no longer raises `ValueError` in `InstancesAPI.retrieve`.

## [6.14.0] - 2023-08-14
### Changed
- Don't terminate client.time_series.subscriptions.iterate_data() when `has_next=false` as more data
may be returned in the future. Instead we return the `has_next` field in the batch, and let the user
decide whether to terminate iteration. This is a breaking change, but this particular API is still
in beta and thus we reserve the right to break it without bumping the major version.

## [6.13.3] - 2023-08-14
### Fixed
- Fixed bug in `ViewApply.properties` had type hint `ConnectionDefinition` instead of `ConnectionDefinitionApply`.
- Fixed bug in `dump` methods of `ViewApply.properties` causing the return code `400` with message
  `Request had 1 constraint violations. Please fix the request and try again. [type must not be null]` to be returned
  from the CDF API.

## [6.13.2] - 2023-08-11
### Fixed
- Fixed bug in `Index.load` that would raise `TypeError` when trying to load `indexes`, when an unexpected field was
  encountered (e.g. during a call to `client.data_modeling.container.list`).

## [6.13.1] - 2023-08-09
### Fixed
- Fixed bug when calling a `retrieve`, `list`, or `create` in `client.data_modeling.container` raised a `TypeError`.
  This is caused by additions of fields to the API, this is now fixed by ignoring unknown fields.

## [6.13.0] - 2023-08-07
### Fixed
- Fixed a bug raising a `KeyError` when calling `client.data_modeling.graphql.apply_dml` with an invalid `DataModelingId`.
- Fixed a bug raising `AttributeError` in `SpaceList.to_space_apply_list`, `DataModelList.to_data_model_apply_list`,
  `ViewList.to_view_apply`. These methods have also been renamed to `.as_apply` for consistency
  with the other data modeling resources.

### Removed
- The method `.as_apply` from `ContainerApplyList` as this method should be on the `ContainerList` instead.

### Added
- Missing `as_ids()` for `DataModelApplyList`, `ContainerList`, `ContainerApplyList`, `SpaceApplyList`, `SpaceList`,
  `ViewApplyList`, `ViewList`.
- Added helper method `.as_id` to `DMLApplyResult`.
- Added helper method `.latest_version` to `DataModelList`.
- Added helper method `.as_apply` to `ContainerList`.
- Added container classes `NodeApplyList`, `EdgeApplyList`, and `InstancesApply`.

## [6.12.2] - 2023-08-04
### Fixed
- Certain errors that were previously silently ignored in calls to `client.data_modeling.graphql.apply_dml` are now properly raised (used to fail as the API error was passed nested inside the API response).

## [6.12.1] - 2023-08-03
### Fixed
- Changed the structure of the GraphQL query used when updating DML models through `client.data_modeling.graphql.apply_dml` to properly handle (i.e. escape) all valid symbols/characters.

## [6.12.0] - 2023-07-26
### Added
- Added option `expand_metadata` to `.to_pandas()` method for list resource types which converts the metadata (if any) into separate columns in the returned dataframe. Also added `metadata_prefix` to control the naming of these columns (default is "metadata.").

## [6.11.1] - 2023-07-19
### Changed
- Return type `SubscriptionTimeSeriesUpdate` in `client.time_series.subscriptions.iterate_data` is now required and not optional.

## [6.11.0] - 2023-07-19
### Added
- Support for Data Point Subscription, `client.time_series.subscriptions`. Note this is an experimental feature.


## [6.10.0] - 2023-07-19
### Added
- Upsert method for `assets`, `events`, `timeseries`, `sequences`, and `relationships`.
- Added `ignore_unknown_ids` flag to `client.sequences.delete`

## [6.9.0] - 2023-07-19
### Added
- Basic runtime validation of ClientConfig.project

## [6.8.7] - 2023-07-18
### Fixed
- Dumping of `Relationship` with `labels` is not `yaml` serializable. This is now fixed.

## [6.8.6] - 2023-07-18
### Fixed
- Include `version` in __repr__ for View and DataModel

## [6.8.5] - 2023-07-18
### Fixed
- Change all implicit Optional types to explicit Optional types.

## [6.8.4] - 2023-07-12
### Fixed
- `max_worker` limit match backend for `client.data_modeling`.

## [6.8.3] - 2023-07-12
### Fixed
- `last_updated_time` and `created_time` are no longer optional on InstanceApplyResult

## [6.8.2] - 2023-07-12
### Fixed
- The `.dump()` method for `InstanceAggregationResult` caused an `AttributeError` when called.

## [6.8.1] - 2023-07-08
### Changed
- The `AssetHierarchy` class would consider assets linking their parent by ID only as orphans, contradicting the
  docstring stating "All assets linking a parent by ID are assumed valid". This is now true (they are no longer
  considered orphans).

## [6.8.0] - 2023-07-07
### Added
- Support for annotations reverse lookup.

## [6.7.1] - 2023-07-07
### Fixed
- Needless function "as_id" on View as it was already inherited
### Added
- Flag "all_versions" on data_modeling.data_models.retrieve() to retrieve all versions of a data model or only the latest one
- Extra documentation on how to delete edges and nodes.
- Support for using full Node and Edge objects when deleting instances.

## [6.7.0] - 2023-07-07
### Added
- Support for applying graphql dml using `client.data_modeling.graphql.apply_dml()`.

## [6.6.1] - 2023-07-07
### Improved
- Added convenience function to instantiate a `CogniteClient.default(...)` to save the users from typing the
  default URLs.

## [6.6.0] - 2023-07-06
### Fixed
- Support for query and sync endpoints across instances in the Data Modeling API with the implementation
  `client.data_modeling.instances`, the methods `query` and `sync`.

## [6.5.8] - 2023-06-30
### Fixed
- Serialization of `DataModel`. The bug caused `DataModel.load(data_model.dump(camel_case=True))` to fail with
  a `TypeError`. This is now fixed.

## [6.5.7] - 2023-06-29
### Fixed
- A bug caused by use of snake case in field types causing `NodeApply.dump(camel_case=True)`
  trigger a 400 response from the API.

## [6.5.6] - 2023-06-29
### Fixed
- A bug causing `ClientConfig(debug=True)` to raise an AttributeError

## [6.5.5] - 2023-06-28
### Fixed
- A bug where we would raise the wrong exception when errors on occurred on `data_modeling.spaces.delete`
- A bug causing inconsistent MRO in DataModelList

## [6.5.4] - 2023-06-28
### Added
- Missing query parameters:
     * `inline_views` in `data_modeling.data_models.retrieve()`.
     * `include_global` in `data_modeling.spaces.list()`.
     * `include_inherited_properties` in `data_modeling.views.retrieve()`.

## [6.5.3] - 2023-06-28
### Fixed
- Only validate `space` and `external_id` for `data_modeling` write classes.


## [6.5.2] - 2023-06-27
### Fixed
- Added missing `metadata` attribute to `iam.Group`

## [6.5.1] - 2023-06-27
### Fixed
- Fix typehints on `data_modeling.instances.aggregate()` to not allow Histogram aggregate.
- Moved `ViewDirectRelation.source` property to `MappedProperty.source` where it belongs.

## [6.5.0] - 2023-06-27
### Added
- Support for searching and aggregating across instances in the Data Modeling API with the implementation
  `client.data_modeling.instances`, the methods `search`, `histogram` and `aggregate`.

## [6.4.8] - 2023-06-23
### Fixed
- Handling non 200 responses in `data_modeling.spaces.apply`, `data_modeling.data_models.apply`,
  `data_modeling.views.apply` and `data_modeling.containers.apply`

## [6.4.7] - 2023-06-22
### Fixed
- Consistently return the correct id types in data modeling resource clients

## [6.4.6] - 2023-06-22
### Fixed
- Don't swallow keyword args on Apply classes in Data Modeling client

## [6.4.5] - 2023-06-21
### Added
- Included tuple-notation when retrieving or listing data model instances

### Improved
- Fixed docstring for retrieving data model instances and extended the examples.

## [6.4.4] - 2023-06-21
Some breaking changes to the datamodeling client. We don't expect any more breaking changes,
but we accept the cost of breaking a few consumers now early on the really nail the user experience.
### Added
- ViewId:as_property_ref and ContainerId:as_property_ref to make it easier to create property references.

### Changed
- Renamed ViewCore:as_reference and ContainerCore:as_reference to :as_id() for consistency with other resources.
- Change Instance:properties to be a `MutableMapping[ViewIdentifier, MutableMapping[PropertyIdentifier, PropertyValue]]`, in order to make it easier to consume
- Make VersionedDataModelingId:load accept `tuple[str, str]`
- Rename ConstraintIdentifier to Constraint - it was not an id but the definition itself
- Rename IndexIdentifier to Index - it was not an id but the definition itself
- Rename ContainerPropertyIdentifier to ContainerProperty - it was not an id but the definition itself

### Removed
- Redundant EdgeApply:create method. It simply mirrored the EdgeApply constructor.


## [6.4.3] - 2023-06-15
### Added
- Accept direct relation values as tuples in `EdgeApply`

## [6.4.2] - 2023-06-15
### Changed
- When providing ids as tuples in `instances.retrieve` and `instances.delete` you should not
have to specify the instance type in each tuple

### Fixed
- Bug where edges and nodes would get mixed up on `instances.retrieve`

## [6.4.1] - 2023-06-14
### Fixed
- Add the missing page_count field for diagram detect items.

## [6.4.0] - 2023-06-12
### Added
- Partial support for the instance resource in the Data Modeling API with the implementation
  `client.data_modeling.instances`, the endpoints `list`, `delete`, `retrieve`, and `apply`

## [6.3.2] - 2023-06-08
### Fixed
- Requests being retried around a token refresh cycle, no longer risk getting stuck with an outdated token.

### Added
- `CredentialProviders` subclassing `_OAuthCredentialProviderWithTokenRefresh`, now accepts a new parameter, `token_expiry_leeway_seconds`, controlling how early a token refresh request should be initiated (before it expires).

### Changed
- `CredentialProviders` subclassing `_OAuthCredentialProviderWithTokenRefresh` now uses a safer default of 15 seconds (up from 3 sec) to control how early a token refresh request should be initiated (before it expires).

## [6.3.1] - 2023-06-07
### Fixed
- Signature of `client.data_modeling.views.retrieve` and `client.data_modeling.data_models.retrieve` to always return a list.

## [6.3.0] - 2023-06-07
### Added
- Support for the container resource in the Data Modeling API with the implementation `client.data_modeling.containers`.
- Support for the view resource in the Data Modeling API with the implementation `client.data_modeling.views`.
- Support for the data models resource in the Data Modeling API with the implementation `client.data_modeling.data_models`.

### Removed
- Removed `retrieve_multiple` from the `SpacesAPI` to have a consistent API with the `views`, `containers`, and `data_models`.

## [6.2.2] - 2023-06-05
### Fixed
- Creating function schedules with current user credentials now works (used to fail at runtime with "Could not fetch a valid token (...)" because a session was never created.)

## [6.2.1] - 2023-05-26
### Added
- Data model centric support in transformation

## [6.2.0] - 2023-05-25
### Added
- Support for the spaces resource in the Data Modeling API with the implementation `client.data_modeling.spaces`.

### Improved
- Reorganized documentation to match API documentation.

## [6.1.10] - 2023-05-22
### Fixed
- Data modelling is now GA. Renaming instance_nodes -> nodes and instance_edges -> edges to make the naming in SDK consistent with Transformation API and CLI

## [6.1.9] - 2023-05-16
### Fixed
- Fixed a rare issue with datapoints fetching that could raise `AttributeError` when running with `pyodide`.

## [6.1.8] - 2023-05-12
### Fixed
- ExtractionPipelinesRun:dump method will not throw an error when camel_case=True anymore

## [6.1.7] - 2023-05-11
### Removed
- Removed DMS v2 destination in transformations

## [6.1.6] - 2023-05-11
### Fixed
- `FunctionsAPI.create` now work in Wasm-like Python runtimes such as `pyodide`.

## [6.1.5] - 2023-05-10
### Fixed
- When creating a transformation with a different source- and destination CDF project, the project setting is no longer overridden by the setting in the `CogniteClient` configuration allowing the user to read from the specified source project and write to the specified and potentially different destination project.

## [6.1.4] - 2023-05-08
### Fixed
- Pickling a `CogniteClient` instance with certain `CredentialProvider`s no longer causes a `TypeError: cannot pickle ...` to be raised.

## [6.1.3] - 2023-05-08
### Added
- Add the license of the package in poetry build.

## [6.1.2] - 2023-05-04
### Improved
- The SDK has received several minor bugfixes to be more user-friendly on Windows.

### Fixed
- The utility function `cognite.client.utils.datetime_to_ms` now raises an understandable `ValueError` when unable to convert pre-epoch datetimes.
- Several functions reading and writing to disk now explicitly use UTF-8 encoding

## [6.1.1] - 2023-05-02
### Fixed
- `AttributeError` when passing `pandas.Timestamp`s with different timezones (*of which one was UTC*) to `DatapointsAPI.retrieve_dataframe_in_tz`.
- A `ValueError` is no longer raised when passing `pandas.Timestamp`s in the same timezone, but with different underlying implementations (e.g. `datetime.timezone.utc` / `pytz.UTC` / `ZoneInfo("UTC")`) to `DatapointsAPI.retrieve_dataframe_in_tz`.

## [6.1.0] - 2023-04-28
### Added
- Support for giving `start` and `end` arguments as `pandas.Timestamp` in `DatapointsAPI.retrieve_dataframe_in_tz`.

### Improved
- Type hints for the `DatapointsAPI` methods.

## [6.0.2] - 2023-04-27
### Fixed
- Fixed a bug in `DatapointsAPI.retrieve_dataframe_in_tz` that could raise `AmbiguousTimeError` when subdividing the user-specified time range into UTC intervals (with fixed offset).

## [6.0.1] - 2023-04-20
### Fixed
- Fixed a bug that would cause `DatapointsAPI.retrieve_dataframe_in_tz` to raise an `IndexError` if there were only empty time series in the response.

## [6.0.0] - 2023-04-19
### Removed
- Removed support for legacy auth (API keys, service accounts, login.status)
- Removed the deprecated `extractionPipeline` argument to `client.extraction_pipelines.create`. Only `extraction_pipeline` is accepted now.
- Removed the deprecated `client.datapoints` accessor attribute. The datapoints API can only be accessed through `client.time_series.data` now.
- Removed the deprecated `client.extraction_pipeline_runs` accessor attribute. The extraction pipeline run API can only be accessed through `client.extraction_pipelines.runs` now.
- Removed the deprecated `external_id` attribute on `ExtractionPipelineRun`. This has been replaced with `extpipe_external_id`.

## [5.12.0] - 2023-04-18
### Changed
- Enforce that types are explicitly exported in order to make very strict type checkers happy.

## [5.11.1] - 2023-04-17
### Fixed
- List (and `__call__`) methods for assets, events, files, labels, relationships, sequences and time series now raise if given bad input for `data_set_ids`, `data_set_external_ids`, `asset_subtree_ids` and `asset_subtree_external_ids` instead of ignoring/returning everything.

### Improved
- The listed parameters above have silently accepted non-list input, i.e. single `int` (for `ids`) or single `str` (for `external_ids`). Function signatures and docstrings have now been updated to reflect this "hidden functionality".

## [5.11.0] - 2023-04-17
### Added
- The `DatapointsAPI` now supports time zones with the addition of a new method, `retrieve_dataframe_in_tz`. It does not support individual customization of query parameters (for good reasons, e.g. a DataFrame has a single index).
- Asking for datapoints in a specific time zone, e.g. `America/New_York` or `Europe/London` is now easily accomplished: the user can just pass in their `datetimes` localized to their time zone directly.
- Queries for aggregate datapoints are also supported, with the key feature being automatic handling of daylight savings time (DST) transitions, as this is not supported by the official API. Example usage: A user living in Oslo, Norway, wants daily averages in their local time. In Oslo, the standard time is UTC+1, with UTC+2 during the summer. This means during spring, there is a 23-hour long day when clocks roll 1 hour forward and a 25-hour day during fall.
- New granularities with a longer time span have been added (only to this new method, for now): 'week', 'month', 'quarter' and 'year'. These do not all represent a fixed frequency, but like the example above, neither does for example 'day' when we use time zones without a fixed UTC offset.

## [5.10.5] - 2023-04-13
### Fixed
- Subclasses of `VisionResource` inheriting `.dump` and `to_pandas` now work as expected for attributes storing lists of subclass instances like `Polygon`, `PolyLine`, `ObjectDetection` or `VisionExtractPredictions` directly or indirectly.

## [5.10.4] - 2023-04-13
### Fixed
- A lot of nullable integer attributes ended up as float after calling `.to_pandas`. These are now correctly converted to `dtype=Int64`.

## [5.10.3] - 2023-04-13
### Fixed
- When passing `CogniteResource` classes (like `Asset` or `Event`) to `update`, any labels were skipped in the update (passing `AssetUpdate` works). This has been fixed for all Cognite resource classes.

## [5.10.2] - 2023-04-12
### Fixed
- Fixed a bug that would cause `AssetsAPI.create_hierarchy` to not respect `upsert=False`.

## [5.10.1] - 2023-04-04
### Fixed
- Add missing field `when` (human readable version of the CRON expression) to `FunctionSchedule` class.

## [5.10.0] - 2023-04-03
### Fixed
- Implemented automatic retries for connection errors by default, improving the reliability of the connection to the Cognite API.
- Added a user-readable message to `CogniteConnectionRefused` error for improved user experience.

### Changed
- Introduce a `max_retries_connect` attribute on the global config, and default it to 3.

## [5.9.3] - 2023-03-27
### Fixed
- After creating a schedule for a function, the returned `FunctionSchedule` was missing a reference to the `CogniteClient`, meaning later calls to `.get_input_data()` would fail and raise `CogniteMissingClientError`.
- When calling `.get_input_data()` on a `FunctionSchedule` instance, it would fail and raise `KeyError` if no input data was specified for the schedule. This now returns `None`.

## [5.9.2] - 2023-03-27
### Fixed
- After calling e.g. `.time_series()` or `.events()` on an `AssetList` instance, the resulting resource list would be missing the lookup tables that allow for quick lookups by ID or external ID through the `.get()` method. Additionally, for future-proofing, the resulting resource list now also correctly has a `CogniteClient` reference.

## [5.9.1] - 2023-03-23
### Fixed
- `FunctionsAPI.call` now also works for clients using auth flow `OAuthInteractive`, `OAuthDeviceCode`, and any user-made subclass of `CredentialProvider`.

### Improved
- `FunctionSchedulesAPI.create` now also accepts an instance of `ClientCredentials` (used to be dictionary only).

## [5.9.0] - 2023-03-21
### Added
- New class `AssetHierarchy` for easy verification and reporting on asset hierarchy issues without explicitly trying to insert them.
- Orphan assets can now be reported on (orphan is an asset whose parent is not part of the given assets). Also, `AssetHierarchy` accepts an `ignore_orphans` argument to mimic the old behaviour where all orphans were assumed to be valid.
- `AssetsAPI.create_hierarchy` now accepts two new parameters: `upsert` and `upsert_mode`. These allow the user to do "insert or update" instead of an error being raised when trying to create an already existing asset. Upsert mode controls whether updates should replace/overwrite or just patch (partial update to non-null values only).
- `AssetsAPI.create_hierarchy` now also verifies the `name` parameter which is required and that `id` has not been set.

### Changed
- `AssetsAPI.create_hierarchy` now uses `AssetHierarchy` under the hood to offer concrete feedback on asset hierarchy issues, accessible through attributes on the raised exception, e.g. invalid assets, duplicates, orphans, or any cyclical asset references.

### Fixed
- `AssetsAPI.create_hierarchy`...:
  - Now respects `max_workers` when spawning worker threads.
  - Can no longer raise `RecursionError`. Used to be an issue for asset hierarchies deeper than `sys.getrecursionlimit()` (typically set at 1000 to avoid stack overflow).
  - Is now `pyodide` compatible.

## [5.8.0] - 2023-03-20
### Added
- Support for client certificate authentication to Azure AD.

## [5.7.4] - 2023-03-20
### Added
- Use `X-Job-Token` header for contextualization jobs to reduce required capabilities.

## [5.7.3] - 2023-03-14
### Improved
- For users unknowingly using a too old version of `numpy` (against the SDK dependency requirements), an exception could be raised (`NameError: name 'np' is not defined`). This has been fixed.

## [5.7.2] - 2023-03-10
### Fixed
- Fix method dump in TransformationDestination to ignore None.

## [5.7.1] - 2023-03-10
### Changed
- Split `instances` destination type of Transformations to `nodes` and `edges`.

## [5.7.0] - 2023-03-08
### Removed
- `ExtractionPipelineRunUpdate` was removed as runs are immutable.

### Fixed
- `ExtractionPipelinesRunsAPI` was hiding `id` of runs because `ExtractionPipelineRun` only defined `external_id` which doesn't exist for the "run resource", only for the "parent" ext.pipe (but this is not returned by the API; only used to query).

### Changed
- Rename and deprecate `external_id` in `ExtractionPipelinesRunsAPI` in favour of the more descriptive `extpipe_external_id`. The change is backwards-compatible, but will issue a `UserWarning` for the old usage pattern.

## [5.6.4] - 2023-02-28
### Added
- Input validation on `DatapointsAPI.[insert, insert_multiple, delete_ranges]` now raise on missing keys, not just invalid keys.

## [5.6.3] - 2023-02-23
### Added
- Make the SDK compatible with `pandas` major version 2 ahead of release.

## [5.6.2] - 2023-02-21
### Fixed
- Fixed an issue where `Content-Type` was not correctly set on file uploads to Azure.

## [5.6.1] - 2023-02-20
### Fixed
- Fixed an issue where `IndexError` was raised when a user queried `DatapointsAPI.retrieve_latest` for a single, non-existent time series while also passing `ignore_unknown_ids=True`. Changed to returning `None`, inline with other `retrieve` methods.

## [5.6.0] - 2023-02-16
### Added
- The SDK has been made `pyodide` compatible (to allow running natively in browsers). Missing features are `CredentialProvider`s with token refresh and `AssetsAPI.create_hierarchy`.

## [5.5.2] - 2023-02-15
### Fixed
- Fixed JSON dumps serialization error of instances of `ExtractionPipelineConfigRevision` and all subclasses (`ExtractionPipelineConfig`) as they stored a reference to the CogniteClient as a non-private attribute.

## [5.5.1] - 2023-02-14
### Changed
- Change `CredentialProvider` `Token` to be thread safe when given a callable that does token refresh.

## [5.5.0] - 2023-02-10
### Added
- Support `instances` destination type on Transformations.

## [5.4.4] - 2023-02-06
### Added
- Added user warnings when wrongly calling `/login/status` (i.e. without an API key) and `/token/inspect` (without OIDC credentials).

## [5.4.3] - 2023-02-05
### Fixed
- `OAuthDeviceCode` and `OAuthInteractive` now respect `global_config.disable_ssl` setting.

## [5.4.2] - 2023-02-03
### Changed
- Improved error handling (propagate IDP error message) for `OAuthDeviceCode` and `OAuthInteractive` upon authentication failure.

## [5.4.1] - 2023-02-02
### Fixed
- Bug where create_hierarchy would stop progressing after encountering more than `config.max_workers` failures.

## [5.4.0] - 2023-02-02
### Added
- Support for aggregating metadata keys/values for assets

## [5.3.7] - 2023-02-01
### Improved
- Issues with the SessionsAPI documentation have been addressed, and the `.create()` have been further clarified.

## [5.3.6] - 2023-01-30
### Changed
- A file-not-found error has been changed from `TypeError` to `FileNotFoundError` as part of the validation in FunctionsAPI.

## [5.3.5] - 2023-01-27
### Fixed
- Fixed an atexit-exception (`TypeError: '<' not supported between instances of 'tuple' and 'NoneType'`) that could be raised on PY39+ after fetching datapoints (which uses a custom thread pool implementation).

## [5.3.4] - 2023-01-25
### Fixed
- Displaying Cognite resources like an `Asset` or a `TimeSeriesList` in a Jupyter notebook or similar environments depending on `._repr_html_`, no longer raises `CogniteImportError` stating that `pandas` is required. Instead, a warning is issued and `.dump()` is used as fallback.

## [5.3.3] - 2023-01-24
### Added
- New parameter `token_cache_path` now accepted by `OAuthInteractive` and `OAuthDeviceCode` to allow overriding location of token cache.

### Fixed
- Platform dependent temp directory for the caching of the token in `OAuthInteractive` and `OAuthDeviceCode` (no longer crashes at exit on Windows).

## [5.3.2] - 2023-01-24
### Security
- Update `pytest` and other dependencies to get rid of dependency on the `py` package (CVE-2022-42969).

## [5.3.1] - 2023-01-20
### Fixed
- Last possible valid timestamp would not be returned as first (if first by some miracle...) by the `TimeSeries.first` method due to `end` being exclusive.

## [5.3.0] - 2023-01-20
### Added
- `DatapointsAPI.retrieve_latest` now support customising the `before` argument, by passing one or more objects of the newly added `LatestDatapointQuery` class.

## [5.2.0] - 2023-01-19
### Changed
- The SDK has been refactored to support `protobuf>=3.16.0` (no longer requires v4 or higher). This was done to fix dependency conflicts with several popular Python packages like `tensorflow` and `streamlit` - and also Azure Functions - that required major version 3.x of `protobuf`.

## [5.1.1] - 2023-01-19
### Changed
- Change RAW rows insert chunk size to make individual requests faster.

## [5.1.0] - 2023-01-03
### Added
- The diagram detect function can take file reference objects that contain file (external) id as well as a page range. This is an alternative to the lists of file ids or file external ids that are still possible to use. Page ranges were not possible to specify before.

## [5.0.2] - 2022-12-21
### Changed
- The valid time range for datapoints has been increased to support timestamps up to end of the year 2099 in the TimeSeriesAPI. The utility function `ms_to_datetime` has been updated accordingly.

## [5.0.1] - 2022-12-07
### Fixed
- `DatapointsArray.dump` would return timestamps in nanoseconds instead of milliseconds when `convert_timestamps=False`.
- Converting a `Datapoints` object coming from a synthetic datapoints query to a `pandas.DataFrame` would, when passed `include_errors=True`, starting in version `5.0.0`, erroneously cast the `error` column to a numeric data type and sort it *before* the returned values. Both of these behaviours have been reverted.
- Several documentation issues: Missing methods, wrong descriptions through inheritance and some pure visual/aesthetic.

## [5.0.0] - 2022-12-06
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

## [4.11.3] - 2022-11-17
### Fixed
- Fix FunctionCallsAPI filtering

## [4.11.2] - 2022-11-16
### Changed
- Detect endpoint (for Engineering Diagram detect jobs) is updated to spawn and handle multiple jobs.
### Added
- `DetectJobBundle` dataclass: A way to manage multiple files and jobs.

## [4.11.1] - 2022-11-15
### Changed
- Update doc for Vision extract method
- Improve error message in `VisionExtractJob.save_annotations`

## [4.11.0] - 2022-10-17
### Added
- Add `compute` method to `cognite.client.geospatial`

## [4.10.0] - 2022-10-13
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

## [4.3.0] - 2022-09-06
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
been several reports of envvar name clash issues in tools built on top the SDK. We therefore
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

### Added
- Add ignore_unknown_ids parameter to files.retrieve_multiple

## [3.5.0] - 2022-08-10
### Changed
- Improve type annotations. Use overloads in more places to help static type checkers.

## [3.4.3] - 2022-08-10
### Changed
- Cache result from pypi version check so it's not executed for every client instantiation.

## [3.4.2] - 2022-08-09
### Fixed
- Fix the wrong destination name in transformations.

## [3.4.1] - 2022-08-01
### Fixed
- fixed exception when printing exceptions generated on transformations creation/update.

## [3.4.0] - 2022-07-25
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

### Changed
- Reverted the optimizations introduced to datapoints fetching in 2.47.0 due to buggy implementation.

## [2.51.0] - 2022-06-13
### Added
- added the new geo_location field to the Asset resource

## [2.50.2] - 2022-06-09
### Fixed
- Geospatial: fix FeatureList.from_geopandas issue with optional properties

## [2.50.1] - 2022-06-09
### Fixed
- Geospatial: keep feature properties as is

## [2.50.0] - 2022-05-30
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

## [2.43.0] - 2022-03-24
### Added
- new list parameters added to `transformations.list`.

## [2.42.0] - 2022-02-25
### Added
- FeatureList.from_geopandas() improvements

### Fixed
- example for templates view.

## [2.41.0] - 2022-02-16
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

## [2.38.6] - 2022-01-14
### Added
- add the possibility to cancel transformation jobs.

## [2.38.5] - 2022-01-12
### Fixed
- Bug where creating/updating/deleting more than 5 transformation schedules in a single call would fail.

## [2.38.4] - 2021-12-23
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

## [2.38.0] - 2021-12-06
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
- Added support for `data_set_id` on template views

### Security
- Disallow downloading files to path outside download directory in `files.download()`.

## [2.32.0] - 2021-10-04
### Added
 - Support for extraction pipelines

## [2.31.1] - 2021-09-30
### Fixed
- Fixed a bug related to handling of binary response payloads.

## [2.31.0] - 2021-08-26
### Added
- View resolver for template fields.

## [2.30.0] - 2021-08-25
### Added
- Support for Template Views

## [2.29.0] - 2021-08-16
### Added
- Raw rows are retrieved using parallel cursors when no limit is set.

## [2.28.2] - 2021-08-12
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

## [2.21.0] - 2021-06-21

### Added
- Datasets support for labels

## [2.20.0] - 2021-06-18

### Added
- rows() in RawRowsAPI support filtering with `columns` and `min/maxLastUpdatedTime`

## [2.19.0] - 2021-05-11

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


## [2.17.1] - 2021-04-15

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


## [2.16.0] - 2021-03-26

### Added
- support for templates.
- date-based `cdf-version` header.

## [2.15.0] - 2021-03-22

### Added
- `createdTime` field on raw dbs and tables.

## [2.14.0] - 2021-03-18

### Added
- dropna argument to insert_dataframe method in DatapointsAPI

## [2.13.0] - 2021-03-16

### Added
- `sortByNodeId` and `partitions` query parameters to `list_nodes` method.

## [2.12.2] - 2021-03-11

### Fixed
- CogniteAPIError raised (instead of internal KeyError) when inserting a RAW row without a key.

## [2.12.1] - 2021-03-09

### Fixed
- CogniteMissingClientError raised when creating relationship with malformed body.

## [2.12.0] - 2021-03-08

### Changed
- Move Entity matching API from beta to v1.

## [2.11.1] - 2021-02-18

### Changed
- Resources are now more lenient on which types they accept in for labels
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

## [2.6.3] - 2020-11-10

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

## [2.5.0] - 2020-09-29

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

## [2.2.0] - 2020-08-17

### Added
- Files labelling support

## [2.1.2] - 2020-08-13

### Fixed
- Fixed a bug where only v1 endpoints (not playground) could be added as retryable

## [2.1.1] - 2020-08-13

### Fixed
- Calls to datapoints `retrieve_dataframe` with `complete="fill"` would break using Pandas version 1.1.0 because it raises TypeError when calling `.interpolate(...)` on a dataframe with no columns.

## [2.1.0] - 2020-07-22

### Added
- Support for passing a single string to `AssetUpdate().labels.add` and `AssetUpdate().labels.remove`. Both a single string and a list of strings is supported. Example:
  ```python
  # using a single string
  my_update = AssetUpdate(id=1).labels.add("PUMP").labels.remove("VALVE")
  res = client.assets.update(my_update)

  # using a list of strings
  my_update = AssetUpdate(id=1).labels.add(["PUMP", "ROTATING_EQUIPMENT"]).labels.remove(["VALVE"])
  res = client.assets.update(my_update)
  ```

## [2.0.0] - 2020-07-21

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
  res = client.assets.update(my_update)
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

## [1.4.10] - 2020-01-27
### Added
- Support for the error field for synthetic time series query in the experimental client.
- Support for retrieving data from multiple sequences at once.

## [1.4.9] - 2020-01-08

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
