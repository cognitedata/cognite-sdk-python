## Development Instructions

### Setup

Get the code!

```bash
git clone https://github.com/cognitedata/cognite-sdk-python.git
cd cognite-sdk-python
```

We use [poetry](https://pypi.org/project/poetry/) for dependency- and virtual environment management. Make sure you use python 3.8.

Install dependencies and initialize a shell within the virtual environment, with these commands:

```bash
poetry install -E all
poetry shell
```

Install pre-commit hooks to run static code checks on every commit:

```bash
pre-commit install
```

You can also manually trigger the static checks with:

```bash
pre-commit run --all-files
```

### Getting access to python-sdk-test CDF project for running integration tests

- Request access to python-sdk AAD tenant.
- Set environment variables as per the interactive login flow below

### Environment Variables

Set the following environment variables in a .env file:

```bash
COGNITE_CLIENT_NAME=python-sdk-integration-tests-<your-name>
COGNITE_PROJECT=python-sdk-test
COGNITE_BASE_URL=https://greenfield.cognitedata.com

# There are three ways of configuring auth against the test project.
# 1) Interactive flow. If you don't have a specific reason to do otherwise, this is the method you
# should use.
LOGIN_FLOW=interactive
COGNITE_TOKEN_SCOPES=https://greenfield.cognitedata.com/.default
COGNITE_AUTHORITY_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea
COGNITE_CLIENT_ID=6b0b4266-ffa4-4b9b-8e13-ddbbc8a19ea6

# 2) Client credentials flow. To run tests which require client credentials to be set
# (such as transformations).
#LOGIN_FLOW=client_credentials
#COGNITE_TOKEN_SCOPES=https://greenfield.cognitedata.com/.default
#COGNITE_TOKEN_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea/oauth2/v2.0/token
#COGNITE_CLIENT_ID=6b0b4266-ffa4-4b9b-8e13-ddbbc8a19ea6
#COGNITE_CLIENT_SECRET=...

# 3) Client certificate flow. To run with client certificate auth.
#LOGIN_FLOW=client_certificate
#COGNITE_TOKEN_SCOPES=https://greenfield.cognitedata.com/.default
#COGNITE_AUTHORITY_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea
#COGNITE_CLIENT_ID=14fd282e-f77a-457d-add5-928ec2bcbf04
#COGNITE_CERT_THUMBPRINT=...
#COGNITE_CERTIFICATE=aadappcert.pem
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
                        --cov cognite
```

Open `htmlcov/index.html` in the browser to navigate through the report.

To speed up test runs pass the following arguments (this will parallelize across 4 processes):

```
pytest -n4 --dist loadscope tests
```

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
