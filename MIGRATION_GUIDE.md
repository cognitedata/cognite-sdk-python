# Migration Guide
Changes are grouped as follows:
- `Dependency` for new hard requirements that you must make sure work with your project before upgrading
- `Deprecated` for methods/features that are still available, but will be removed in a future release
- `Removed` for methods/features that have been removed (and what to do now!)
- `Function Signature` for methods/functions with a change to its function signature, e.g. change in default values
- `Functionality` for changes that are not backwards compatible
- `Changed` for changes that do not fall into any other category
- `Optional` for new, optional methods/features that you should be aware of - *and could take advantage of*

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
- DataClass `ViewDirectRelation`, `ContainerDirectRelation` replaced by `DirectRelation`.
- DataClasses `MappedPropertyDefinition`, `MappedApplyPropertyDefinition` replaced by `MappedProperty`, `MappedPropertyApply`.
- DataClasses `RequiresConstraintDefinition` and `UniquenessConstraintDefinition` replaced by `RequiresConstraint` and `UniquenessConstraint`.

### Function Signature
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
- The `client.sequences.data.retrieve` method has changed signature:
  The parameter `columns_external_id` is renamed `columns`. This is to better match the API and have a consistent overload implementation.
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
-The class `SequenceRows` in `client.data_classes.transformations.common` has been renamed to `SequenceRowsDestination`.
- Classes `Geometry`, `AssetAggregate`, `AggregateResultItem`, `EndTimeFilter`, `Label`, `LabelFilter`, `ExtractionPipelineContact`,
  `TimestampRange`, `AggregateResult`, `GeometryFilter`, `GeoLocation`, `RevisionCameraProperties`, `BoundingBox3D` are no longer
  `dict` but classes with attributes matchhng the API.

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
