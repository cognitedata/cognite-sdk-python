import argparse
import os
import re

import requests


def check_if_version_exists(package_name: str, version: str):
    versions = get_all_versions(package_name=package_name)
    return version in versions


def get_newest_version_in_major_release(package_name: str, version: str):
    major, minor, micro, pr_cycle, pr_version = _parse_version(version)
    versions = get_all_versions(package_name)
    for v in versions:
        if _is_newer_major(v, version):
            major, minor, micro, pr_cycle, pr_version = _parse_version(v)
    return _format_version(major, minor, micro, pr_cycle, pr_version)


def get_all_versions(package_name: str):
    disable_ssl = os.getenv("COGNITE_DISABLE_SSL", False)
    verify_ssl = not disable_ssl
    res = requests.get("https://pypi.python.org/simple/{}/#history".format(package_name), verify=verify_ssl, timeout=5)
    versions = re.findall("cognite-sdk-(\d+\.\d+.[\dabrc]+)", res.content.decode())
    return versions


def _is_newer_major(version_a, version_b):
    major_a, minor_a, micro_a, pr_cycle_a, pr_version_a = _parse_version(version_a)
    major_b, minor_b, micro_b, pr_cycle_b, pr_version_b = _parse_version(version_b)
    is_newer = False
    if major_a == major_b and minor_a >= minor_b and micro_a >= micro_b:
        if minor_a > minor_b:
            is_newer = True
        else:
            if micro_a > micro_b:
                is_newer = True
            else:
                is_newer = _is_newer_pre_release(pr_cycle_a, pr_version_a, pr_cycle_b, pr_version_b)
    return is_newer


def _is_newer_pre_release(pr_cycle_a, pr_v_a, pr_cycle_b, pr_v_b):
    cycles = ["a", "b", "rc", None]
    assert pr_cycle_a in cycles, "pr_cycle_a must be one of '{}', not '{}'.".format(pr_cycle_a, cycles)
    assert pr_cycle_b in cycles, "pr_cycle_a must be one of '{}', not '{}'.".format(pr_cycle_b, cycles)
    is_newer = False
    if cycles.index(pr_cycle_a) > cycles.index(pr_cycle_b):
        is_newer = True
    elif cycles.index(pr_cycle_a) == cycles.index(pr_cycle_b):
        if pr_v_a is not None and pr_v_b is not None and pr_v_a > pr_v_b:
            is_newer = True
    return is_newer


def _parse_version(version: str):
    pattern = "(\d+)\.(\d+)\.(\d+)(?:([abrc]+)(\d+))?"
    match = re.match(pattern, version)
    if not match:
        raise ValueError("Could not parse version {}".format(version))
    major, minor, micro, pr_cycle, pr_version = match.groups()
    return int(major), int(minor), int(micro), pr_cycle, int(pr_version) if pr_version else None


def _format_version(major, minor, micro, pr_cycle, pr_version):
    pr_cycle = pr_cycle or ""
    pr_version = pr_version or ""
    return "{}.{}.{}{}{}".format(major, minor, micro, pr_cycle, pr_version)


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
