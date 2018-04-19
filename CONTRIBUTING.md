## Development Instructions
### Setup
```bash
$ git clone https://github.com/cognitedata/cognite-sdk-python.git
$ cd cognite-sdk-python
$ pipenv install -d
$ pipenv shell
```

### Testing
Set up tests for all new functionality. Running the tests will require setting the environment variable 'COGNITE_TEST_API_KEY'.

Initiate unit tests by running the following command from the root directory:

`$ pytest`

If you want to generate code coverage reports run:

```
pytest --cov-report html \
       --cov-report xml \
       --cov cognite
```

Open `htmlcov/index.html` in the browser to navigate through the report.

### Documentation
Build html files of documentation locally by running
```bash
$ cd docs
$ make html
```
Documentation will be automatically generated from the google-style docstrings in the source code. It is then built and released when changes are merged into master.

### Release version conventions
TODO: Add more detailed instructions on releasing
Format:
```
MAJOR.MINOR[.PATCH]
```
Increment the:
* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards-compatible manner, and
* PATCH version when you make backwards-compatible bug fixes.

For more information on versioning see https://semver.org/