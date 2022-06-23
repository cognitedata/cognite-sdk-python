#!/bin/bash
# This file is temporary and will be removed in the future.

cat ../cognite-sdk-python-experimental/cognite/experimental/_api/functions.py | sed 's/experimental/client/g' | sed 's/api\/playground\//api\/v1\//g' > cognite/client/_api/functions.py
echo "Updated functions.py"

cat ../cognite-sdk-python-experimental/cognite/experimental/data_classes/functions.py | sed 's/experimental/client/g'  | sed 's/api\/playground\//api\/v1\//g' > cognite/client/data_classes/functions.py
echo "Updated data_classes/functions.py"

cat ../cognite-sdk-python-experimental/cognite/experimental/_constants.py | sed 's/experimental/client/g' > cognite/client/_constants.py
echo "Updated _constants.py"

# cat ../cognite-sdk-python-experimental/tests/tests_unit/test_api/test_functions.py | sed 's/experimental/client/g' > tests/tests_unit/test_api/test_functions.py
# echo "Updated api test_functions.py"

cat ../cognite-sdk-python-experimental/tests/tests_unit/test_data_classes/test_functions.py | sed 's/experimental/client/g' > tests/tests_unit/test_data_classes/test_functions.py
echo "Updated data test_functions.py"

echo "=============================================================="
echo "Copy test folders to tests/tests_unit/test_api/function_test_resources"
echo "Update Cognite.rst"
echo "Update _version.py"
echo "Update CHANGELOG.md"