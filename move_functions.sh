#!/bin/bash
# This file is temporary and will be removed in the future.

cat ../cognite-sdk-python-experimental/cognite/experimental/_api/functions.py | sed 's/experimental/client/g' | sed 's/api\/playground\//api\/v1\//g' > cognite/client/_api/functions.py
echo "Updated functions.py"

cat ../cognite-sdk-python-experimental/cognite/experimental/data_classes/functions.py | sed 's/experimental/client/g'  | sed 's/api\/playground\//api\/v1\//g' > cognite/client/data_classes/functions.py
echo "Updated data_classes/functions.py"

cat ../cognite-sdk-python-experimental/cognite/experimental/_constants.py | sed 's/experimental/client/g' > cognite/client/_constants.py
echo "Updated _constants.py"

cat ../cognite-sdk-python-experimental/tests/tests_unit/test_api/test_functions.py | sed 's/experimental/client/g' > tests/tests_unit/test_api/test_functions.py
echo "Updated api test_functions.py"

cat ../cognite-sdk-python-experimental/tests/tests_unit/test_data_classes/test_functions.py | sed 's/experimental/client/g' > tests/tests_unit/test_data_classes/test_functions.py
echo "Updated data test_functions.py"

mkdir -p tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/bad_absolute_import tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/bad_function_code tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/bad_function_code2 tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/function_code tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/good_absolute_import tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/relative_imports tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/function_code_with_requirements tests/tests_unit/test_api/function_test_resources
cp -r ../cognite-sdk-python-experimental/tests/tests_unit/test_api/function_code_with_invalid_requirements tests/tests_unit/test_api/function_test_resources
cp ../cognite-sdk-python-experimental/tests/tests_unit/test_api/handler.py tests/tests_unit/test_api/function_test_resources
cp ../cognite-sdk-python-experimental/tests/tests_unit/test_api/__init__.py tests/tests_unit/test_api/function_test_resources
echo "Copied test folders to tests/tests_unit/test_api/function_test_resources"

echo "=============================================================="
echo "Update Cognite.rst"
code ../cognite-sdk-python-experimental/docs/source/cognite.rst
code docs/source/cognite.rst

echo "Update _version.py"
code cognite/client/_version.py

echo "Update CHANGELOG.md"
code CHANGELOG.md

echo "=============================================================="
echo "Validate the changes before commiting"