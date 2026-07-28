## Development Instructions

### Setup

Get the code!

```bash
git clone https://github.com/cognitedata/cognite-sdk-python
cd cognite-sdk-python
```

We use [poetry](https://pypi.org/project/poetry/) for dependency- and virtual environment management. Make sure you use python 3.10.

Install dependencies and initialize a shell within the virtual environment, with these commands:

```bash
poetry install -E all
poetry env activate
```

Note that `poetry env activate` only prints out the command, you have to run it yourself (or wrap it in `eval`). You may also prefix commands with `poetry run`.

Install pre-commit hooks to run various checks and linters on every commit:

```bash
pre-commit install
```

You can also manually trigger the static checks with:

```bash
pre-commit run --all-files
```

To include slower checks that run in CI, include `--hook-stage manual`

```bash
pre-commit run --all-files --hook-stage manual
```

### Getting access to `python-sdk-contributor` CDF project for running integration tests

- Request access to python-sdk AAD tenant. Cognite internals can tag the Python SDK Maintainer shield on Slack.
- Set environment variables as per the interactive login flow below

### Environment Variables

Set the following environment variables in a .env file:

```bash
COGNITE_CLIENT_NAME=python-sdk-integration-tests-<your-name>
COGNITE_PROJECT=python-sdk-contributor
COGNITE_BASE_URL=https://api.cognitedata.com

# There are three ways of configuring auth against the test project.
# 1) Interactive flow. If you don't have a specific reason to do otherwise, this is the method you
# should use.
LOGIN_FLOW=interactive
COGNITE_TOKEN_SCOPES=https://api.cognitedata.com/.default
COGNITE_AUTHORITY_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea
COGNITE_CLIENT_ID=6b0b4266-ffa4-4b9b-8e13-ddbbc8a19ea6

# 2) Client credentials flow. To run tests which require client credentials to be set
# (such as transformations).
#LOGIN_FLOW=client_credentials
#COGNITE_TOKEN_SCOPES=https://api.cognitedata.com/.default
#COGNITE_TOKEN_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea/oauth2/v2.0/token
#COGNITE_CLIENT_ID=6b0b4266-ffa4-4b9b-8e13-ddbbc8a19ea6
#COGNITE_CLIENT_SECRET=...

# 3) Client certificate flow. To run with client certificate auth.
#LOGIN_FLOW=client_certificate
#COGNITE_TOKEN_SCOPES=https://api.cognitedata.com/.default
#COGNITE_AUTHORITY_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea
#COGNITE_CLIENT_ID=14fd282e-f77a-457d-add5-928ec2bcbf04
#COGNITE_CERT_THUMBPRINT=...
#COGNITE_CERTIFICATE=aadappcert.pem
```

### Automatic Code Generation

The main code base in this repository is written for the `AsyncCogniteClient`. From this source code we also create a
"sync" `CogniteClient` through automatic code generation. The async->sync conversion script can be found in `scripts/sync_client_codegen/main.py`. It exclusively writes to `cognite/client/_sync_api/` (and temporary files as part of normalized file compares).

The script stores the hash of file it was created from in the module docstring, in order to quickly skip files that don't need to be updated. A variety of updates does not result in actual code changes in the SyncAPIs, as it only wraps the async client directly.

We have certain rules in place that must be followed:

- **Adding nested APIs: Put one API class per file.**

If the entire API addition is "flat", like classic resource types EventsAPI or FilesAPI, please use a single file directly below `cognite/client/_api/`. Else, add a directory instead. There are plenty of existing examples, e.g. IAMAPI, RawAPI.

#### Run modes
`usage: main.py [-h] [-v] {verify,run} ...`

Use `verify` to ensure all sync source code files are up-to-date and no stale files (that should be removed exists). This is mostly useful as a CI check.

Use `run` to pass a subset of _async_ source files (i.e. anything below `cognite/client/_api/`) to update:

```sh
python scripts/sync_client_codegen/main.py run --files FILE1 FILE2 ...
```
Note: This is how we run it as part of `pre-commit`.

To run through all files, pass `--all-files`. This will also run cleanup automatically:

```sh
python scripts/sync_client_codegen/main.py run --all-files
```

If a sync API file has entered into a bad state (e.g. through manual changes), you can simply delete it (or modify
the hash) to have it re-generated from scratch.

Verbose mode is supported (e.g. debugging). Pass `-v` before the subcommand `run`/`verify`:

```sh
python scripts/sync_client_codegen/main.py -v [run, verify] ...
```

### Testing

Initiate unit tests by running the following command from the root directory:

`pytest tests/tests_unit`

If you have appropriate credentials (see [Environment Variables](#environment-variables)), you can run the integration tests like this:

`pytest tests/tests_integration`

If you want to generate code coverage reports run:

```
pytest tests/tests_unit --cov-report html \
                        --cov-report xml \
                        --cov cognite tests
```

Open `htmlcov/index.html` in the browser to navigate through the report.

To speed up test runs pass the following arguments (this will parallelize across 4 processes):

```
pytest -n4 --dist loadscope tests
```

#### Unit Tests for Examples in Documentation

For code examples defined in *docstrings* the doctest library is used and docstring tests are defined in `tests/tests_unit/test_docstring_examples.py`. Some docstring code examples may require patching which should be done here.

For any code examples written directly in `docs/source` we are using the [sphinx doctest extension](https://www.sphinx-doc.org/en/master/usage/extensions/doctest.html) with pytest. See the `docs/source/quickstart.rst` for an example of a unit test that is setup to use some fixtures defined through pytest (`docs/source/conftest.py`). To run all the tests defined in docs run:

```
pytest docs
```

### Updating Integration Runner Auth

If you need to add a new capability to the integration runner, you need to create a new Pull Requests (PR) in which
you do the following:

1. Add a read-only version of the new capability to the `scripts/toolkit/access/auth/readonly.Group.yaml`
2. Add a read-write version of the new capability to the `scripts/toolkit/access/auth/readwrite.Group.yaml`

Get the PR reviewed by an SDK maintainer. The integration runner will be updated once the PR is merged.

For an illustration of the setup see [diagram](https://miro.com/app/board/uXjVIbOg5zw=/).

### Documentation

Build html files of documentation locally by running

```bash
cd docs
make html
```

Open `build/html/index.html` to look at the result.

Documentation will be automatically generated from the google-style docstrings in the source code.
It is then built and released when changes are merged into master.

### Release version conventions

See https://semver.org/

### Automated release process
We automate the release process using [release-please](https://github.com/googleapis/release-please).

With this, all PRs must use the Conventional Commit spec for the title. If a fix/feat/perf commit is merged to master, release-please creates a PR to bump versions and update the changelog accordingly.

The following commit types will cause a release-please PR to be created:
```
fix -> patch bump
feat -> minor bump
feat!/fix!/perf! -> major bump (! indicates breaking change)
```
