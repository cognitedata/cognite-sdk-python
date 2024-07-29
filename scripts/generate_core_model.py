"""This script requires pygen to be installed. It generates typed classes for the core data model v1.

`pip install cognite-pygen==0.99.28`

Note that `pygen` requires `Python 3.9` or later`, so if you develop in an older version of Python,
you need to run this script in a Python 3.9 environment.
"""

from pathlib import Path

from cognite.pygen._generator import generate_typed
from tests.tests_integration.conftest import make_cognite_client

THIS_REPO = Path(__file__).resolve().parent.parent

OUTPUT_FILE = THIS_REPO / "cognite" / "client" / "data_classes" / "cdm" / "v1.py"


def main() -> None:
    client = make_cognite_client(beta=False)

    generate_typed(("cdf_cdm_experimental", "core_data_model", "v1"), OUTPUT_FILE, client, format_code=False)


if __name__ == "__main__":
    main()
