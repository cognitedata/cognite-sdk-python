import argparse
import re

import requests


def check_if_version_exists(package_name: str, version: str):
    res = requests.get("https://pypi.python.org/simple/{}/#history".format(package_name))
    versions = re.findall("cognite-sdk-(\d+\.\d+.[\dabrc]+)", res.content.decode())
    return version in versions


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--package", required=True)
    parser.add_argument("-v", "--version", required=True)
    args = parser.parse_args()

    version_exists = check_if_version_exists(args.package, args.version)

    if version_exists:
        print("yes")
    else:
        print("no")
