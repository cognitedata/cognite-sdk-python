# Migration Guide
Changes are grouped as follows:
- `Dependency` for new hard requirements that you must make sure work with your project before upgrading
- `Deprecated` for methods/features that are still available, but will be removed in a future release
- `Removed` for methods/features that have been removed (and what to do now!)
- `Function Signature` for methods/functions with a change to its function signature, e.g. change in default values
- `Functionality` for changes that are not backwards compatible
- `Changed` for changes that do not fall into any other category
- `Optional` for new, optional methods/features that you should be aware of - *and could take advantage of*

## From v7 to v8
### Dependency
- **httpx**: The SDK now uses `httpx` instead of `requests` for HTTP operations. This is a new required dependency that
  enables the new async interface.

### Optional
- **Async Support**: The SDK now provides full async support. The main client is now `AsyncCogniteClient`, but the synchronous `CogniteClient` is still available for backward compatibility. An important implementation detail is that it just wraps `AsyncCogniteClient`.
- All helper/utility methods on data classes now have an async variant. A few examples: Class `Asset` has `children` and now also `children_async`, `subtree` and `subtree_async`, class `Function` now has `call` and `call_async`, class `TimeSeries` now has `latest` and `latest_async` etc.
- Instantiating a client has gotten a tiny bit simpler, by allowing either `cluster` or `base_url` to be passed. When passing cluster, it is expected to be on the form 'https://{cluster}.cognitedata.com'
- The context manager `FileMultipartUploadSession`, returned by a call to one of the Files API methods multipart_upload_content` or `multipart_upload_content_session`, now also supports async; you can enter with `async with`, and upload parts using `await upload_part_async`.
- The SDK now ships with a new mock for the async client, namely `AsyncCogniteClientMock`. Both it and the previous `CogniteClientMock` are greatly improved and provide better type safety, checking of call signatures and spec_set=True is now enforced for all APIs (even the mocked client itself), through the use of `create_autospec` and bottom-up construction of nested APIs.
- With the move to an async client, concurrency now works in Pyodide e.g. Jupyter-Lite in the browser. This also means that user interfaces like Streamlit won't freeze while resources from CDF are being fetched!
- Good to know: File upload speeds are now significantly faster on Windows after `requests` are gone!

### Removed
- The generic `aggregate` method on classic CDF APIs has been removed (Assets, Events, Sequences and Time Series). Use one of the more specific `aggregate_count`, `aggregate_unique_values`, `aggregate_cardinality_values`, `aggregate_cardinality_properties`, or `aggregate_unique_properties` instead.
- The generic `aggregate` method on the Data Sets API and Files API has been replaced with `aggregate_count`.
- The aggregation data class `CountAggregate` has been removed. Methods now return the count (`int`) directly.
- The generic `filter` method on classic CDF APIs has been removed (Assets, Events, Sequences and Time Series). Use the normal `list` method instead and pass filters as `advanved_filters=...` instead.
- Datapoints API method `retrieve_dataframe_in_tz` has been removed. Use `retrieve`, `retrieve_arrays` or `retrieve_dataframe` and specify `timezone` instead.
- The method `trigger` on the Workflow Executions API has been removed. Use `run` instead.
- The method `create` on the Workflow Triggers API has been removed. Use `upsert` instead.
- The method `get_triggers` on the Workflow Triggers API has been removed. Use `list` instead.
- The method `get_trigger_run_history` on the Workflow Triggers API has been removed. Use `list_runs` instead.
- Data class `WorkflowTriggerCreate` has been removed. Use `WorkflowTriggerUpsert`.
- Data class `DataPointSubscriptionCreate` has been removed. Use `DataPointSubscriptionWrite`.
- The method `as_primitive` method on the identifier data class `WorkflowVersionId` has been removed. Use `as_tuple` instead.
- The methods `insert`, `retrieve`, `retrieve_last_row` and `retrieve_dataframe` on the Sequence Data API no longer support the parameter `column_external_ids`. Use `columns` instead.
- The `__iter__` method has been removed from all APIs. Use `__call__` instead: `for ts in client.time_series()`. This makes it seamless to pass one or more parameters.
- All references to `legacy_name` on time series data classes and API have been removed.
- The helper methods on `client.iam`, `compare_capabilities` and `verify_capabilities` no longer support the `ignore_allscope_meaning` parameter.
- The method `load_yaml` on the data class `Query` has been removed. Use `load` instead.
- The Templates API has been completely removed from the SDK (the API service has already been shut off)
- The separate beta `CogniteClient` has been removed. Note: APIs currently in alpha/beta are (already) implemented directly on the main client and throw warnings on use.

### Changed
- Attributes on all "read" data classes now have the correct type (typically no longer `Optional[...]`), meaning type inference will be correct. If you try to instantiate these classes directly (*you shouldn't* - use the write versions instead!), you will see that all required parameters in the API response will also be required on the object. **What is a read class?** Any data class returned by the SDK from a call to the API to fetch a resource of some kind.
- All typed instance apply classes, e.g. `CogniteAssetApply` from `cognite.client.data_classes.data_modeling.cdm.v1` (or `extractor_extensions.v1`) now work with patch updates (using `replace=False`). Previously, all unset fields would be dumped as `None` and thus cleared/nulled in the backend database. Now, any unset fields are not dumped and will not clear an existing value (unless used with `replace=True`).
- When using the Datapoints API to ingest datapoints through `insert_dataframe`, the parameters `external_id_headers` and `instance_id_headers` have been removed. The new logic infers the kind of identifier from the type of the column: an integer is an ID, a string is an external ID and a NodeId (or 2-tuple of space and ext.id) is an instance ID. This also means you can pass more than one type of time series identifier in the same pandas DataFrame.
- Datapoints API method `retrieve_dataframe` and all `to_pandas` methods on datapoints-container-like objects now accept a new parameter: `include_unit` (`bool`). Time series using physical units via `unit_external_id`, will end up as part of the pandas DataFrame columns (like aggregate info).
- The `.columns` of the returned pandas DataFrame from both Datapoints API method `retrieve_dataframe` and all `to_pandas` methods on datapoints-container-like objects are now pandas `MultiIndex`, where each level corresponds to the: time series identifiers (always present), then the possible aggregate-, granularity-, status code- and unit information. Any level with no information is automatically dropped. This solves several practical problems in the past when the identifiers were always cast to string before e.g. `|interpolation` was appended directly to it. This worked ok until `NodeId` appeared. Now, the identifiers are never cast. If you have a `node_id`, you can extract the time series directly from the dataframe: `df[node_id]`. If you fetch several aggregates, you can select one just as easily: `df[node_id, "average"]`.
- When using the Datapoints API to request datapoints, passing dictionaries with individual settings is no longer supported. Pass raw identifiers (`str`, `int`, `NodeId`) or `DatapointsQuery`.
- When using the Datapoints API to ingest datapoints, empty containers no longer raise `ValueError`, but short-circuit.
- Passing `column_names` to the Datapoints API method `retrieve_dataframe` or to `to_pandas` on any datapoints-container-like instance is no longer supported. The resolving is now dynamic with the precedence order: instance ID, then external ID and lastly (internal) ID.
- For users of the Data Modeling API method `sync`, the data classes have been split from those used in `query`. They can be recognized by simply appending `Sync` to the end, e.g. `Query` and `QuerySync`. Previously, these were used for both, but as these API endpoints continue to evolve, it makes sense to fine-tune the data classes to each - starting now. Examples: `QuerySync`, `SelectSync`, `NodeResultSetExpressionSync` and `EdgeResultSetExpressionSync`.
- When using `to_pandas` on a list of Data Modeling instances, the properties will be expanded by default (to separate columns). To get the old behaviour, pass `expand_properties=False`.
- When using `to_pandas` on a list of Data Modeling instances, parameters `expand_metadata` and `metadata_prefix` are no longer silently ignored and will raise as unrecognized.
- When using `get` on a list of Data Modeling instances, the parameter `id` has been removed. Use `instance_id` (or `external_id` when there is no ambiguity on space).
- Parameter `partitions` has been removed from all `__call__` methods except for the Raw Rows API (which has special handling for it). It was previosuly being ignored with the added side effect of ignoring `chunk_size` stemming from a very early API design oversight.
- The method `retrieve` on the Workflow Versions API no longer accepts `workflow_external_id` and `version` as separate arguments. Pass a single or a sequence of `WorkflowVersionId` (tuples also accepted).
- When loading a `ViewProperty` or `ViewPropertyApply`, the resource dictionary must contain the `"connectionType"` key or an error is raised.
- The specific exceptions `CogniteDuplicatedError` and `CogniteNotFoundError` should now always be used when appropriate (previously certain API endpoints always used `CogniteAPIError`)
- `ModelFailedException` has changed name to `CogniteModelFailedError`.
- For `class Transformation`, which used to have an async `run` method, this is now named `run_async` to unify the overall interface. The same applies to the `cancel` and `jobs` methods for the same class, and `update` and `wait` on `TransformationJob`.
- Iterating through a `DatapointsArray` is no longer supported. Access the numpy arrays directly and use vectorised operations instead.
- Extending a `Datapoints` instance is no longer supported.
- **ClientConfig**:
  - `max_workers` has now permanently moved to `global_config` after the deprecation period
  - `timeout`: default has been increased from 30 sec to 60 sec
- **global_config**:
  - New setting `follow_redirects` that controls whether or not to follow redirects. Defaults to `False`.
  - New setting `file_download_chunk_size` that allows you to override the chunk size for streaming file downloads. Defaults to `None` (auto).
  - New setting `file_upload_chunk_size` that allows you to override the chunk size for streaming file uploads.
  - New setting `event_loop`, allowing you to override the default event loop used by the SDK
  - `proxies` have been replaced by `proxy` and follow httpx directly. See: [Proxies - HTTPX](https://www.python-httpx.org/advanced/proxies/)
  - `max_retry_backoff`: default has been increased from 30 sec to 60 sec
  - `max_connection_pool_size`: default has been reduced from 50 to 20

### Deprecated
- Accessing the Sequence Data API through the Sequence API should use `.data` instead of `.rows`. This may raise in a future version.

## From v6 to v7
### Functionality
- `CogniteResource.to_pandas` and `CogniteResourceList.to_pandas` now converts known timestamps to `datetime` by
  default. Can be turned off with the new parameter `convert_timestamps`. Note: To comply with older pandas v1, the
  dtype will always be `datetime64[ns]`, although in v2 this could have been `datetime64[ms]`.
- Read operations, like `retrieve_multiple` will now fast-fail. Previously, all requests would be executed
  before the error was raised, potentially fetching thousands of unneccesary resources.

### Deprecated
- The Templates API is deprecated, and will be removed in a future version. Please migrate to Data Modeling.
  Read more at: https://docs.cognite.com/cdf/data_modeling/
- The `client.assets.aggregate` use `client.assets.aggregate_count` instead.
- The `client.events.aggregate` use `client.events.aggregate_count` instead.
- The `client.sequence.aggregate` use `client.sequence.aggregate_count` instead.
- The `client.time_series.aggregate` use `client.time_series.aggregate_count` instead.

### Removed
- Deprecated method `aggregate_metadata_keys` on AssetsAPI. Update your query from
  `aggregate_metadata_keys(filter=my_filter)` to `aggregate_unique_properties("metadata", filter=my_filter)`.
- Deprecated method `aggregate_metadata_values` on AssetsAPI. Update your query from
  `aggregate_metadata_values(keys=["country"], filter=my_filter)` to
  `aggregate_unique_values(["metadata", "country"], filter=my_filter)`.
- Deprecated method `update_feature_types` on GeospatialAPI, use `patch_feature_types` instead.
- The `SequenceColumns` no longer set the `external_id` to `column{no}` if it is missing. It now must be set
  explicitly by the user.
- Dataclasses `ViewDirectRelation` and `ContainerDirectRelation` are replaced by `DirectRelation`.
- Dataclasses `MappedPropertyDefinition` and `MappedApplyPropertyDefinition` are replaced by `MappedProperty` and `MappedPropertyApply`.
- Dataclasses `RequiresConstraintDefinition` and `UniquenessConstraintDefinition` are replaced by `RequiresConstraint` and `UniquenessConstraint`.
- In data class `Transformation` attributes `has_source_oidc_credentials` and `has_destination_oidc_credentials` are replaced by properties.

### Function Signature
- All `.dump` methods for CogniteResource classes like `Asset` or `Event` now uses `camel_case=True` by default. This is
  to match the intended use case, preparing the object to be sent in an API request.
- `CogniteResource.to_pandas` now more closely resembles `CogniteResourceList.to_pandas` with parameters
  `expand_metadata` and `metadata_prefix`, instead of accepting a sequence of column names (`expand`) to expand,
  with no easy way to add a prefix. Also, it no longer expands metadata by default.
- Additionally, `Asset.to_pandas`, have `expand_aggregates` and `aggregates_prefix`. Since the possible `aggregates`
  keys are known, `camel_case` will also apply to these if expanded as opposed to metadata keys.
- Removed parameters `property` and `aggregates` for method `aggregate_unique_values` on GeospatialAPI, use the
  `output` parameter instead.
- Removed parameter `fields` for method `aggregate_unique_values` on EventsAPI, use the other aggregate-prefixed
  methods instead.
- Removed parameter `function_external_id` for method `create` on FunctionSchedulesAPI (function_id has been
  required since the deprecation of API keys).
- `client.data_modeling.instances.aggregate` the parameters `instance_type` and `group_by` has swapped order.
- The return type of `client.data_modeling.instances.aggregate` has changed from `InstanceAggregationResultList` to
  a more specific value `AggregatedNumberedValue | list[AggregatedNumberedValue] | InstanceAggregationResultList` depending on the `aggregates` and `group_by` parameters.
- The `client.sequences.data` methods `.retrieve` and `.insert`  method has changed signature:
  The parameter `column_external_ids` is renamed `columns`. The old parameter `column_external_ids` is still there, but is
  deprecated. In addition, int the `.retrieve` method, the parameters `id` and `external_id` have
  been moved to the beginning of the signature. This is to better match the API and have a consistent overload
  implementation.
- The `client.sequences.data.retrieve_latest` is renamed `client.sequences.data.retrieve_last_row`.

### Changed
- All `assert`s meant for the SDK user, now raise appropriate errors instead (`ValueError`, `RuntimeError`...).
- `CogniteAssetHierarchyError` is no longer possible to catch as an `AssertionError`.
- More narrow exception types like `CogniteNotFoundError` and `CogniteDuplicatedError` are now raised instead of
  `CogniteAPIError` for the following methods: `DatapointsAPI.retrieve_latest`, `RawRowsAPI.list`,
  `RelationshipsAPI.list`, `SequencesDataAPI.retrieve`, `SyntheticDatapointsAPI.query`. Additionally, all calls
  using `partitions` to API methods like `list` (or the generator version) now do the same.
- Several methods in the data modelling APIs have had parameter names now correctly reflect whether they accept
  a single or multiple items (i.e. id -> ids).
- Passing `limit=0` no longer returns `DEFAULT_LIMIT_READ` (25) resources, but raises a `ValueError`.
- Loading `ObjectDetection` attributes `.attributes`, `.bounding_box`, `.polygon` and
  `.polyline` now returns types `dict[str, Attribute]`, `BoundingBox`, `Polygon` and `Polyline` instead of `dicts`.
- Loading `TextRegion` attribute `.text_region` now return `BoundingBox` instead of `dict`.
- Loading `AssetLink` attribute `.text_region` and `.asset_ref` returns `BoundingBox` and `CdfResourceRef` instead of `dict`.
- Loading `KeypointCollection` attributes `.keypoints` and `.attributes` return `dict[str, Keypoint]` and
  `dict[str, Attribute]` instead of `dict`.
- Loading `VisionExtractPredictions` the attributes `text_predictions`, `asset_tag_prediction`,
  `industrial_object_prediction`, `people_predictions`, `personal_protective_equipment_predictions`,
  `digital_gauge_predictions`, `dial_gauge_predictions`, `level_gauge_predictions`, `valve_predictions`
   now returns `dict[str, TextRegion]`, `list[AssetLink]`, `list[ObjectDetection]`, `list[KeypointCollectionWithObjectDetection]` instead of `dict`.
- Loading `FeatureParameters` the attributes `text_detection_parameters`, `asset_tag_detection_parameters`,
  `industrial_object_prediction_parameters`, `personal_protective_equipment_parameters`,
  `digital_gauge_parameters`, `dial_gauge_detection_parameters`, `level_gauge_parameters`, `valve_detection_parameters`
   now returns `TextDetectionParameter`, `AssetTagDetectionParameters`, `PeopleDetectionParameters`,
  `IndustrialObjectDetectionParameters`, `PersonalProtectiveEquimentDetectionParameters`, `DigitalGaugeDetection`,
  `ValveDetection` instead of `dict`.
- Loading `ExtractionPipeline` the attribute `.contacts` now returns `list[ExtractionPipelineContact]` instead of `dict`.
- Loading `GeospatialComputedResponse` the attribute `.items` now returns `GeospatialComputedItemList` instead of `list[dict]`.
- Loading `Transformation` the attributes `.running_job`, `.last_finished_job`, `.blocked`, `.schedule` `.source_session`,
  `.destination_session`, `.source_nonce`, `destination_nonce`, `.source_oidc_credentials`, `.destination_oidc_credentials`
  now returns `TransformationJob`, `TransformationBlockedInfo`, `TransformationSchedule`, `SessionDetails`, `NonceCredentials`
  `OidcCredentials`, instead of `dict`s.
- Loading `TransformationPreviewResult` the attribute `.schema` now returns `TransformationSchemaColumnList` instead of `list[dict]`.
- Loading `TransformationJob` the attribute `.destination` and `.status` now return `TransformationDestination` and `TransformationJobStatus` instead of `dict`.
- The `Group` attribute `capabilities` is now a `Capabilities` object, instead of a `dict`.
- The class `SequenceData` has been replaced by `SequenceRows`. The old `SequenceData` class is still available for
  backwards compatibility, but will be removed in the next major version. However, all API methods now return
  `SequenceRows` instead of `SequenceData`.
- The attribute `columns` in `Sequence` has been changed from `SequenceType[dict]` to `SequnceColumnList`.
- The class `SequenceRows` in `client.data_classes.transformations.common` has been renamed to `SequenceRowsDestination`.
- Classes `Geometry`, `AssetAggregate`, `AggregateResultItem`, `EndTimeFilter`, `Label`, `LabelFilter`, `ExtractionPipelineContact`,
  `TimestampRange`, `AggregateResult`, `GeometryFilter`, `GeoLocation`, `RevisionCameraProperties`, `BoundingBox3D` are no longer
  `dict` but classes with attributes matchhng the API.
- Calling `client.iam.token.inspect()` now gives an object `TokenInspection` with attribute `cababilities` of type `ProjectCapabilityList`
  instead of `list[dict]`
- In data class `Transformation` the attribute `schedule`, `running_job`, and `last_running_job`, `external_id` and `id`
  are set to the `Transformation` `id` and `external_id` if not set. If they are set to a different value, a `ValueError` is raised

### Optional
- `CogniteImportError` can now be caught as `ImportError`.

## From v5 to v6
### Removed
- Removed support for legacy auth (API keys, service accounts, client.login.status()). Use OIDC to authenticate instead and use `client.iam.token.inspect()` instead of `login.status()`.
- Removed the deprecated `extractionPipeline` argument to `client.extraction_pipelines.create`. Only `extraction_pipeline` is accepted now.
- Removed the deprecated `client.datapoints` accessor attribute. The datapoints API can only be accessed through `client.time_series.data` now.
- Removed the deprecated `client.extraction_pipeline_runs` accessor attribute. The extraction pipeline run API can only be accessed through `client.extraction_pipelines.runs` now.
- Removed the deprecated `external_id` attribute on `ExtractionPipelineRun`. This has been replaced with `extpipe_external_id`.

## From v4 to v5
### Dependency
- Required dependency, `protobuf`.
- Optional dependency, `numpy` (performance benefits, but requires refactoring).

### Deprecated
- Consider changing `client.datapoints` (deprecated) to `client.time_series.data`.

### Removed
- `DatapointsAPI.query`: All three retrieve-endpoints now support arbitrarily complex queries. See docstring of `DatapointsAPI.retrieve` for example usage.
- `DatapointsAPI.retrieve_dataframe_dict`: Not needed as the combination of `.get` and e.g. `.average` covers the same use case - with simpler syntax.
- Exception `CogniteDuplicateColumnsError`: Duplicated identifiers in datapoint queries are now allowed.
- All convenience methods related to plotting and the use of `matplotlib` (go ask your FNDS <3 (Friendly Neighborhood Data Scientist).

### Function Signature
- All parameters to all retrieve methods are now keyword-only (meaning no positional arguments are supported).
- All Cognite resource types (data classes) have a `to_pandas` (or `to_geopandas`) method. Previously, these had various defaults for the `camel_case` parameter, but they have all been changed to `False`.
- `DatapointsAPI.insert_dataframe` has new default values for:
  - `dropna`: Changed from `False` to `True` (functionality unchanged, applies per-column)
  - `external_id_headers`: Changed from `False` to `True` to disincentivize the use of internal IDs.
- `DatapointsAPI.retrieve_dataframe` no longer has a mix of `id`s and `external_id`s in the columns when given identifiers for both arguments. Instead, the user might select either by passing the new parameter `column_names`.
- `DatapointsAPI.retrieve_dataframe` no longer supports the `complete` parameter.
- `DatapointsAPI.retrieve_dataframe` now accepts the parameter `uniform_index (bool)`. See function docstring for explanation and usage examples.

### Functionality
- Fetching datapoints with `limit=0` no longer returns default number of datapoints, but 0.
- Fetching raw datapoints with a finite `limit` and `include_outside_points=True` now returns both outside points (if they exist) regardless of the given `limit`, meaning the returned datapoints count is up to `limit+2`.
- Fetching aggregate datapoints now includes the time period(s) (given by the `granularity` unit) that `start` and `end` are part of (used to be only the "fully in-between" points).
- The `get` method of `DatapointsList` and `DatapointsArrayList` now returns a list of `Datapoints` (or list of `DatapointsArray`) for all identifiers that were duplicated in the user's query. Previously duplicated identifiers would raise prior to fetching data with one exception: (weird edge case) when passing the `id` and `external_id` for the same time series.
- The ordering of columns with aggregates in `pandas.DataFrame`s as returned by `DatapointsAPI.retrieve_dataframe`, `Datapoints.to_pandas` and `DatapointsList.to_pandas` is now chronological, i.e. `average` before `discrete_variance` which in turn comes before `sum`.
- The utility function `datetime_to_ms` no longer issues a `FutureWarning` on missing timezone information. It will interpret naive `datetime`s as local time as is Python's default interpretation.
- The utility function `ms_to_datetime` no longer issues a `FutureWarning` on returning a naive `datetime` in UTC. It will now return an aware `datetime` object in UTC.
- The utility function `ms_to_datetime` no longer raises `ValueError` for inputs from before 1970, but will raise for input outside the allowed minimum- and maximum supported timestamps in the API.

### Optional
- Resource types `DatapointsArray` and `DatapointsArrayList` which offer way more efficient memory storage for datapoints (uses `numpy.ndarrays`)
- New datapoints fetching method, `DatapointsAPI.retrieve_arrays`, returning `DatapointsArray` and `DatapointsArrayList`.
- A single aggregate, e.g. `average`, can be specified by a string directly, `aggregates="average"` (as opposed to packing it inside a list, `aggregates=["average"]`)


## From v3 to v4
...

## From v2 to v3
...

## From v1 to v2
...
