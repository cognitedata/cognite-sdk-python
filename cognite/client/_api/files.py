import copy
import os
from pathlib import Path
from typing import Iterator, cast, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    FileAggregate,
    FileMetadata,
    FileMetadataFilter,
    FileMetadataList,
    FileMetadataUpdate,
)
from cognite.client.utils._identifier import Identifier, IdentifierSequence


class FilesAPI(APIClient):
    _RESOURCE_PATH = "/files"

    def __call__(
        self,
        chunk_size=None,
        name=None,
        mime_type=None,
        metadata=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        labels=None,
        geo_location=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        source_created_time=None,
        source_modified_time=None,
        uploaded_time=None,
        external_id_prefix=None,
        directory_prefix=None,
        uploaded=None,
        limit=None,
    ):
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            labels=labels,
            geo_location=geo_location,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            uploaded_time=uploaded_time,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            external_id_prefix=external_id_prefix,
            directory_prefix=directory_prefix,
            uploaded=uploaded,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
        )

    def __iter__(self):
        return cast(Iterator[FileMetadata], self())

    def create(self, file_metadata, overwrite=False):
        res = self._post(
            url_path=self._RESOURCE_PATH, json=file_metadata.dump(camel_case=True), params={"overwrite": overwrite}
        )
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata["uploadUrl"]
        file_metadata = FileMetadata._load(returned_file_metadata)
        return (file_metadata, upload_url)

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=FileMetadataList, resource_cls=FileMetadata, identifiers=identifiers)

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(
        self,
        name=None,
        mime_type=None,
        metadata=None,
        asset_ids=None,
        asset_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        labels=None,
        geo_location=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        source_created_time=None,
        source_modified_time=None,
        uploaded_time=None,
        external_id_prefix=None,
        directory_prefix=None,
        uploaded=None,
        limit=25,
    ):
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = FileMetadataFilter(
            name=name,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            labels=labels,
            geo_location=geo_location,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            uploaded_time=uploaded_time,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            external_id_prefix=external_id_prefix,
            directory_prefix=directory_prefix,
            uploaded=uploaded,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)
        return self._list(
            list_cls=FileMetadataList, resource_cls=FileMetadata, method="POST", limit=limit, filter=filter
        )

    def aggregate(self, filter=None):
        return self._aggregate(filter=filter, cls=FileAggregate)

    def delete(self, id=None, external_id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    @overload
    def update(self, item):
        ...

    @overload
    def update(self, item):
        ...

    def update(self, item):
        return self._update_multiple(
            list_cls=FileMetadataList,
            resource_cls=FileMetadata,
            update_cls=FileMetadataUpdate,
            resource_path=self._RESOURCE_PATH,
            items=item,
        )

    def search(self, name=None, filter=None, limit=100):
        return self._search(list_cls=FileMetadataList, search={"name": name}, filter=(filter or {}), limit=limit)

    def upload(
        self,
        path,
        external_id=None,
        name=None,
        source=None,
        mime_type=None,
        metadata=None,
        directory=None,
        asset_ids=None,
        source_created_time=None,
        source_modified_time=None,
        data_set_id=None,
        labels=None,
        geo_location=None,
        security_categories=None,
        recursive=False,
        overwrite=False,
    ):
        file_metadata = FileMetadata(
            name=name,
            directory=directory,
            external_id=external_id,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            asset_ids=asset_ids,
            data_set_id=data_set_id,
            labels=labels,
            geo_location=geo_location,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            security_categories=security_categories,
        )
        if os.path.isfile(path):
            if not name:
                file_metadata.name = os.path.basename(path)
            return self._upload_file_from_path(file_metadata, path, overwrite)
        elif os.path.isdir(path):
            tasks = []
            if recursive:
                for (root, _, files) in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        basename = os.path.basename(file_path)
                        file_metadata = copy.copy(file_metadata)
                        file_metadata.name = basename
                        tasks.append((file_metadata, file_path, overwrite))
            else:
                for file_name in os.listdir(path):
                    file_path = os.path.join(path, file_name)
                    if os.path.isfile(file_path):
                        file_metadata = copy.copy(file_metadata)
                        file_metadata.name = file_name
                        tasks.append((file_metadata, file_path, overwrite))
            tasks_summary = utils._concurrency.execute_tasks(
                self._upload_file_from_path, tasks, self._config.max_workers
            )
            tasks_summary.raise_compound_exception_if_failed_tasks(task_unwrap_fn=(lambda x: x[0].name))
            return FileMetadataList(tasks_summary.results)
        raise ValueError(f"The path '{path}' does not exist")

    def _upload_file_from_path(self, file, file_path, overwrite):
        with open(file_path, "rb") as fh:
            file_metadata = self.upload_bytes(fh, overwrite=overwrite, **file.dump())
        return file_metadata

    def upload_bytes(
        self,
        content,
        name,
        external_id=None,
        source=None,
        mime_type=None,
        metadata=None,
        directory=None,
        asset_ids=None,
        data_set_id=None,
        labels=None,
        geo_location=None,
        source_created_time=None,
        source_modified_time=None,
        security_categories=None,
        overwrite=False,
    ):
        file_metadata = FileMetadata(
            name=name,
            external_id=external_id,
            source=source,
            mime_type=mime_type,
            metadata=metadata,
            directory=directory,
            asset_ids=asset_ids,
            data_set_id=data_set_id,
            labels=labels,
            geo_location=geo_location,
            source_created_time=source_created_time,
            source_modified_time=source_modified_time,
            security_categories=security_categories,
        )
        res = self._post(
            url_path=self._RESOURCE_PATH, json=file_metadata.dump(camel_case=True), params={"overwrite": overwrite}
        )
        returned_file_metadata = res.json()
        upload_url = returned_file_metadata["uploadUrl"]
        headers = {"Content-Type": file_metadata.mime_type}
        self._http_client_with_retry.request(
            "PUT", upload_url, data=content, timeout=self._config.file_transfer_timeout, headers=headers
        )
        return FileMetadata._load(returned_file_metadata)

    def retrieve_download_urls(self, id=None, external_id=None, extended_expiration=False):
        batch_size = 100
        id_batches = [seq.as_dicts() for seq in IdentifierSequence.load(id, external_id).chunked(batch_size)]
        query_params = {}
        if extended_expiration:
            query_params["extendedExpiration"] = True
        tasks = [
            dict(url_path="/files/downloadlink", json={"items": id_batch}, params=query_params)
            for id_batch in id_batches
        ]
        tasks_summary = utils._concurrency.execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        results = tasks_summary.joined_results(unwrap_fn=(lambda res: res.json()["items"]))
        return {(result.get("id") or result["externalId"]): result["downloadUrl"] for result in results}

    def download(self, directory, id=None, external_id=None):
        if isinstance(directory, str):
            directory = Path(directory)
        all_ids = IdentifierSequence.load(id, external_id).as_dicts()
        id_to_metadata = self._get_id_to_metadata_map(all_ids)
        assert directory.is_dir(), f"{directory} is not a directory"
        self._download_files_to_directory(directory, all_ids, id_to_metadata)

    def _get_id_to_metadata_map(self, all_ids):
        ids = [id["id"] for id in all_ids if ("id" in id)]
        external_ids = [id["externalId"] for id in all_ids if ("externalId" in id)]
        files_metadata = self.retrieve_multiple(ids=ids, external_ids=external_ids)
        id_to_metadata = {}
        for f in files_metadata:
            id_to_metadata[f.id] = f
            id_to_metadata[f.external_id] = f
        return id_to_metadata

    def _download_files_to_directory(self, directory, all_ids, id_to_metadata):
        tasks = [(directory, id, id_to_metadata) for id in all_ids]
        tasks_summary = utils._concurrency.execute_tasks(
            self._process_file_download, tasks, max_workers=self._config.max_workers
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda task: id_to_metadata[utils._auxiliary.unwrap_identifer(task[1])]),
            str_format_element_fn=(lambda metadata: metadata.id),
        )

    def _get_download_link(self, identifier):
        return self._post(url_path="/files/downloadlink", json={"items": [identifier]}).json()["items"][0][
            "downloadUrl"
        ]

    def _process_file_download(self, directory, identifier, id_to_metadata):
        id = utils._auxiliary.unwrap_identifer(identifier)
        file_metadata = id_to_metadata[id]
        file_path = (directory / cast(str, file_metadata.name)).resolve()
        file_is_in_download_directory = directory.resolve() in file_path.parents
        if not file_is_in_download_directory:
            raise RuntimeError(f"Resolved file path '{file_path}' is not inside download directory")
        download_link = self._get_download_link(identifier)
        self._download_file_to_path(download_link, file_path)

    def _download_file_to_path(self, download_link, path, chunk_size=(2**21)):
        with self._http_client_with_retry.request(
            "GET", download_link, stream=True, timeout=self._config.file_transfer_timeout
        ) as r:
            with path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

    def download_to_path(self, path, id=None, external_id=None):
        if isinstance(path, str):
            path = Path(path)
        assert path.parent.is_dir(), f"{path.parent} is not a directory"
        identifier = Identifier.of_either(id, external_id).as_dict()
        download_link = self._get_download_link(identifier)
        self._download_file_to_path(download_link, path)

    def download_bytes(self, id=None, external_id=None):
        identifier = Identifier.of_either(id, external_id).as_dict()
        download_link = self._get_download_link(identifier)
        return self._download_file(download_link)

    def _download_file(self, download_link):
        res = self._http_client_with_retry.request("GET", download_link, timeout=self._config.file_transfer_timeout)
        return res.content
