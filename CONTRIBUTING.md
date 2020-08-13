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

### Environment Variables
Set the following environment variables in a .env file:
```bash
COGNITE_CLIENT_NAME=python-sdk-integration-tests
COGNITE_MAX_RETRIES=20

# Only necessary for running integration tests
COGNITE_PROJECT=python-sdk-test
COGNITE_API_KEY=<api-key>
COGNITE_BASE_URL=https://greenfield.cognitedata.com
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
