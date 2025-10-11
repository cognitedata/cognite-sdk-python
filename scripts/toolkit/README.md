# Service Principal Groups Configuration

## Overview

This directory contains the configuration for the Groups used by the 
* Integration Runner service principals.
* Contributors principal.

The CI runs on two different CDF projects:

1. `python-sdk-contributor` this is the CDF Project used by the Contributors
    and all GitHub Actions that have PRs that targets the `master` branch.
2. `python-sdk-test-prod` this is the CDF Project that is used by the GitHub actions that 
    runs on the `main` branch.

There are two groups defined in this directory, one for reading and one for writing. 
The configuration is set up such that:

* Contributors has write access to the `python-sdk-contributor` project
  and read access to the `python-sdk-test-prod` project.
* The integration runner on PRs that targets the `master` branch has write access to the 
  `python-sdk-contributor` project.
* The integration runner on PRs that targets the `main` branch has write access to the 
  `python-sdk-test-prod` project.

## Adding a new ACL

To add a new ACL, you need to:

1. Create a new branch.
2. Add a read version of the ACL to `modules/access/auth/readonly.Group.yaml`.
3. Add a read/write version of the ACL to `modules/access/auth/readwrite.Group.yaml`.
4. Commit and push the changes to your branch.
5. Create a pull request against the `main` branch.
6. Once the PR is approved and merged, the changes will be applied automatically.

See for example the [#2176](https://github.com/cognitedata/cognite-sdk-python/pull/2176)
