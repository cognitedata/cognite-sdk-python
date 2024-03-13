#!/usr/bin/env bash

poetry install -E all

poetry run pre-commit install
