## Development Instructions
### Setup
```bash
$ git clone https://github.com/cognitedata/cognite-sdk-python.git
$ cd cognite-sdk-python
$ pipenv install -d
$ pipenv shell
```

### Unit testing
Set up unit tests for all new functionality
Run unit tests by running the following command from the root directory:

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
Format:
```
MAJOR.MINOR[.MICRO]
```

The major and minor version numbers should mirror the Cognite API. Micro releases are dedicated to bug fixes, improvements, and additions.