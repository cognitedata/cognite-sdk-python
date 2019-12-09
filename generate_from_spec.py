import argparse
import os

from openapi.generator import CodeGenerator

DEFAULT_SPEC_URL = "https://storage.googleapis.com/cognitedata-api-docs/dist/v1.json"
PLAYGROUND_SPEC_URL = "https://storage.googleapis.com/cognitedata-api-docs/dist/playground.json"


def main(spec_url, spec_path):
    codegen = CodeGenerator(spec_url, spec_path)
    codegen_playground = CodeGenerator(
        PLAYGROUND_SPEC_URL, exclude_schemas=["CustomMenuHierarchy", "CustomMenuHierarchyNode"]
    )

    spec = codegen.open_api_spec

    print("=" * 100)
    print("{}: {}".format(spec.info.title, spec.info.version))
    print(spec.info.description)
    print("=" * 100)

    for root, dirs, files in os.walk("./cognite/client/data_classes"):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith(".py"):
                if "relationships" in file_path or "types" in file_path or "assets" in file_path:
                    print("* Generating playground code in {}".format(file_path))
                    codegen_playground.generate(file_path, file_path)
                else:
                    print("* Generating normal code in {}".format(file_path))
                    codegen.generate(file_path, file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="The url or path to the spec you wish to generate from. Defaults to latest spec.")
    parser.add_argument(
        "--path", help="The url or path to the spec you wish to generate from. Defaults to latest spec."
    )
    args = parser.parse_args()
    main(args.url or DEFAULT_SPEC_URL, args.path)
