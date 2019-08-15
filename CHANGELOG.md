# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The changelog for SDK version 0.x.x can be found [here](https://github.com/cognitedata/cognite-sdk-python/blob/0.13/CHANGELOG.md).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [Planned]
- Concurrent reads for all resource types using `/cursors` endpoints
- Upserts for all resource types
- Separate read/write fields on data classes

## [Unreleased]
### Added
- SequencesAPI.list now accepts an asset_ids parameter for searching by asset
- SequencesDataAPI.insert now accepts a SequenceData object for easier copying
- DatapointsAPI.insert now accepts a Datapoints object for easier copying

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
