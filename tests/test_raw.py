import os

import pytest


@pytest.fixture(scope='module')
def databases():
    from cognite.raw import get_databases
    return get_databases(limit=1, api_key=os.getenv('COGNITE_API_KEY'), project='akerbp')


@pytest.fixture(scope='module')
def tables(databases):
    from cognite.raw import get_tables
    db_name = databases.to_json()[0]['dbName']
    return get_tables(database_name=db_name, limit=1, api_key=os.getenv('COGNITE_API_KEY'), project='akerbp')


@pytest.fixture(scope='module')
def rows(databases, tables):
    from cognite.raw import get_rows
    db_name = databases.to_json()[0]['dbName']
    table_name = tables.to_json()[0]['tableName']
    return get_rows(database_name=db_name, table_name=table_name, limit=1, api_key=os.getenv('COGNITE_API_KEY'),
                    project='akerbp')


def test_databases_object(databases):
    from cognite.data_objects import RawObject
    assert isinstance(databases, RawObject)


def test_tables_object(tables):
    from cognite.data_objects import RawObject
    assert isinstance(tables, RawObject)


def test_rows_object(rows):
    from cognite.data_objects import RawObject
    assert isinstance(rows, RawObject)


def test_json(databases):
    assert isinstance(databases.to_json(), list)


def test_pandas(databases):
    import pandas as pd
    assert isinstance(databases.to_pandas(), pd.DataFrame)


def test_ndarray(databases):
    import numpy as np
    assert isinstance(databases.to_ndarray(), np.ndarray)


def test_response_length(databases):
    assert len(databases.to_json()) > 0
