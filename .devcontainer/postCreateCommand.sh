#!/usr/bin/env bash

# Copy in default VSCode settings file as part of initial devcontainer create process.
# This avoids overwriting user-provided settings for normal local dev setups.
mkdir -p .vscode
cp .devcontainer/vscode.default.settings.json .vscode/settings.json

# Configure Poetry to create virtual environments inside the project directory
poetry config virtualenvs.in-project true

poetry env use python3.10
poetry install -E all
poetry run pre-commit install
