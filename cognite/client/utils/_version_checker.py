from __future__ import annotations

import argparse
import functools
import re

import requests

from cognite.client.config import global_config


def check_if_version_exists(package_name: str, version: str) -> bool:
    versions = get_all_versions(package_name=package_name)
    return version in versions


@functools.lru_cache(maxsize=1)
def get_newest_version_in_major_release(package_name: str, version: str) -> str:
    major, minor, micro, pr_cycle, pr_version = _parse_version(version)
    versions = get_all_versions(package_name)
    for v in versions:
        if _is_newer_major(v, version):
            major, minor, micro, pr_cycle, pr_version = _parse_version(v)
    return _format_version(major, minor, micro, pr_cycle, pr_version)


def get_all_versions(package_name: str) -> list[str]:
    verify_ssl = not global_config.disable_ssl
    res = requests.get(f"https://pypi.python.org/simple/{package_name}/#history", verify=verify_ssl, timeout=5)
    return re.findall(r"cognite-sdk-(\d+\.\d+.[\dabrc]+)", res.content.decode())


def _is_newer_major(version_a: str, version_b: str) -> bool:
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


def _is_newer_pre_release(
    pr_cycle_a: str | None, pr_v_a: int | None, pr_cycle_b: str | None, pr_v_b: int | None
) -> bool:
    cycles = ["a", "b", "rc", None]
    assert pr_cycle_a in cycles, f"pr_cycle_a must be one of '{pr_cycle_a}', not '{cycles}'."
    assert pr_cycle_b in cycles, f"pr_cycle_a must be one of '{pr_cycle_b}', not '{cycles}'."
    is_newer = False
    if cycles.index(pr_cycle_a) > cycles.index(pr_cycle_b):
        is_newer = True
    elif cycles.index(pr_cycle_a) == cycles.index(pr_cycle_b):
        if pr_v_a is not None and pr_v_b is not None and pr_v_a > pr_v_b:
            is_newer = True
    return is_newer


def _parse_version(version: str) -> tuple[int, int, int, str, int | None]:
    pattern = r"(\d+)\.(\d+)\.(\d+)(?:([abrc]+)(\d+))?"
    match = re.match(pattern, version)
    if not match:
        raise ValueError(f"Could not parse version {version}")
    major, minor, micro, pr_cycle, pr_version = match.groups()
    return int(major), int(minor), int(micro), pr_cycle, int(pr_version) if pr_version else None


def _format_version(major: int, minor: int, micro: int, pr_cycle: str | None, pr_version: int | None) -> str:
    return f"{major}.{minor}.{micro}{pr_cycle or ''}{pr_version or ''}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--package", required=True)
    parser.add_argument("-v", "--version", required=True)
    args = parser.parse_args()

    version_exists = check_if_version_exists(args.package, args.version)

    if version_exists:
        print("yes")  # noqa: T201
    else:
        print("no")  # noqa: T201
