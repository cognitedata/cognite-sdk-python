import os

import numpy as np
import pandas as pd
import pytest

from cognite.v04 import cloud_storage


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
    from cognite.v04.dto import FileListResponse
    response = cloud_storage.list_files(limit=3)
    assert isinstance(response, FileListResponse)
    assert isinstance(response.to_pandas(), pd.DataFrame)
    assert isinstance(response.to_json(), list)
    assert isinstance(response.to_ndarray(), np.ndarray)
    assert len(response.to_json()) > 0 and len(response.to_json()) <= 3


def test_list_files_empty():
    response = cloud_storage.list_files(source='not_a_source')
    assert response.to_pandas().empty
    assert len(response.to_json()) == 0


@pytest.fixture(scope='module')
def file_id():
    res = cloud_storage.list_files(name='test_file', source='sdk-tests', limit=1)
    return res.to_json()[0]['id']


def test_get_file_info(file_id):
    from cognite.v04.dto import FileInfoResponse
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
