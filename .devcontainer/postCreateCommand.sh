#!/usr/bin/env bash
set -euo pipefail

# Copy in default VSCode settings file as part of initial devcontainer create process.
# This avoids overwriting user-provided settings for normal local dev setups.
mkdir -p .vscode
cp .devcontainer/vscode.default.settings.json .vscode/settings.json

uv sync --python 3.10 --extra all
uv run pre-commit install
