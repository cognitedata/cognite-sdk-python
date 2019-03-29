import argparse
import os

from openapi.generator import CodeGenerator

DEFAULT_SPEC_PATH = "openapi/spec_ts_normal.json"


def main(spec_url, spec_path):
    codegen = CodeGenerator(spec_url, spec_path)
    spec = codegen.open_api_spec

    print("=" * 100)
    print("OpenApi Spec: {}".format(spec.info.title))
    print("Version: {}".format(spec.info.version))
    print(spec.info.description)
    print("=" * 100)

    for root, dirs, files in os.walk("./cognite/client/api"):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith(".py") and os.path.basename(file_path) in ("time_series.py", "datapoints.py"):
                print("* Generating code in {}".format(file_path))
                codegen.generate(file_path, file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="The url or path to the spec you wish to generate from. Defaults to latest spec.")
    parser.add_argument(
        "--path", help="The url or path to the spec you wish to generate from. Defaults to latest spec."
    )
    args = parser.parse_args()
    main(args.url, args.path or DEFAULT_SPEC_PATH)
