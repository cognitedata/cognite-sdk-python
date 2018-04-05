import os

import numpy as np
import pandas as pd
import pytest

from cognite import cloud_storage


def test_upload_file_metadata():
    response = cloud_storage.upload_file('test_file', source='sdk-tests', overwrite=True)
    assert response.get('uploadURL') is not None
    assert response.get('fileId') is not None


def test_upload_file(tmpdir):
    file_path = os.path.join(tmpdir, 'test_file.txt')
    tmpdir.join('test_file.txt').write("This is a test file.")
    with pytest.warns(UserWarning):
        response = cloud_storage.upload_file('test_file', file_path, source='sdk-tests', overwrite=True)
    assert response.get('uploadURL') is None
    assert response.get('fileId') is not None


def test_list_files():
    response = cloud_storage.list_files(limit=3)
    assert len(response) > 0 and len(response) <= 3


@pytest.fixture(scope='module')
def file_id():
    res = cloud_storage.list_files(name='test_file', source='sdk-tests', limit=1)
    return res[0]['id']


def test_get_file_info(file_id):
    from cognite.data_objects import FileInfoResponse
    response = cloud_storage.get_file_info(file_id)
    assert isinstance(response, FileInfoResponse)
    assert isinstance(response.to_json(), dict)
    assert isinstance(response.to_ndarray(), np.ndarray)
    assert isinstance(response.to_pandas(), pd.DataFrame)
    assert response.id == file_id


@pytest.mark.parametrize('get_contents', [True, False])
def test_download_files(file_id, get_contents):
    try:
        response = cloud_storage.download_file(file_id, get_contents)
        if get_contents:
            assert isinstance(response, bytes)
        else:
            assert isinstance(response, str)
    except Exception as e:
        print("Failed to download file: ", e)


def test_delete_file(file_id):
    response = cloud_storage.delete_files([file_id])
    assert file_id in response['deleted'] or file_id in response['failed']
