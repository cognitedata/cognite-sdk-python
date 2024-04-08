#!/usr/bin/env bash

# Copy in default VSCode settings file as part of initial devcontainer create process.
# This instead of a checked-in .vscode/settings.json file, to not overwrite user provided settings for normal local dev setups.
mkdir -p .vscode
cp .devcontainer/vscode.default.settings.json .vscode/settings.json

# Install all dependencies with Poetry
poetry install -E all

# Install pre-commit hook
poetry run pre-commit install
