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

## [Unreleased]
- Concurrent reads for all resource types using `/cursors` endpoints
- Upserts for all resource types
- Separate read/write fields on data classes

## [1.0.0] - 2019-07-11
### Added
- Support for all endpoints in Cognite API
- Generator with hidden cursor for all resource types
- Concurrent writes for all resources
- Distribution of "core" sdk which does not depend on pandas and numpy
- Typehints for all methods
- Support for posting an entire asset hierarchy, resolving ref_id/parent_ref_id automatically
- config attribute on CogniteClient to view current configuration. 

### Removed
- `experimental` client in order to ensure sdk stability.

### Changed
- Renamed methods so they reflect what the method does instead of what http method is used
- Updated documentation with automatically tested examples
- Renamed `stable` namespace to `api`
- Rewrote logic for concurrent reads of datapoints
- Renamed CogniteClient parameter `num_of_workers` to `max_workers`
