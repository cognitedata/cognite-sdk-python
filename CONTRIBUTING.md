## Development Instructions
### Setup

Get the code!
```bash
git clone https://github.com/cognitedata/cognite-sdk-python.git
cd cognite-sdk-python
```
Install dependencies and initialize a shell within the virtual environment.

To get the virtual environment, install [pipenv](https://pypi.org/project/pipenv/). Then execute:

```bash
pipenv shell
pipenv sync -d
```
Install pre-commit hooks
```bash
pre-commit install
```

### Getting access to python-sdk-test tenant for running integration tests
- Request access to python-sdk AAD tenant.
- Set environment variables as per the interactive login flow below

### Environment Variables
Set the following environment variables in a .env file:
```bash
COGNITE_CLIENT_NAME=python-sdk-integration-tests-<your-name>
COGNITE_MAX_RETRIES=20

# Only necessary for running integration tests
LOGIN_FLOW=interactive
COGNITE_PROJECT=python-sdk-test
COGNITE_BASE_URL=https://greenfield.cognitedata.com
COGNITE_AUTHORITY_URL=https://login.microsoftonline.com/dff7763f-e2f5-4ffd-9b8a-4ba4bafba5ea
COGNITE_CLIENT_ID=6b0b4266-ffa4-4b9b-8e13-ddbbc8a19ea6
COGNITE_TOKEN_SCOPES=https://greenfield.cognitedata.com/.default
```

### Testing
Set up tests for all new functionality. Running the tests will require setting the environment variable 'COGNITE_API_KEY'.

Initiate unit tests by running the following command from the root directory:

`pytest tests/tests_unit`

If you have an appropriate API key, you can run the integratino tests like this:

`pytest tests/tests_integration`

If you want to generate code coverage reports run:

```
pytest tests/tests_unit --cov-report html \
                        --cov-report xml \
                        --cov cognite
```

Open `htmlcov/index.html` in the browser to navigate through the report.

### Documentation
Build html files of documentation locally by running
```bash
cd docs
make html
```
Documentation will be automatically generated from the google-style docstrings in the source code. It is then built and released when changes are merged into master.

### Release version conventions
See https://semver.org/
