import numbers
from typing import Any, Dict, List, Sequence, Type, Union

from requests import Response

from cognite.client._api_client import APIClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import (
    ContextualizationJob,
    DiagramConvertResults,
    DiagramDetectResults,
    T_ContextualizationJob,
)
from cognite.client.utils._auxiliary import to_camel_case


class DiagramsAPI(APIClient):
    _RESOURCE_PATH = "/context/diagram"

    def _camel_post(
        self,
        context_path: str,
        json: Dict[str, Any] = None,
        params: Dict[str, Any] = None,
        headers: Dict[str, Any] = None,
    ) -> Response:
        return self._post(
            self._RESOURCE_PATH + context_path,
            json={to_camel_case(k): v for k, v in (json or {}).items() if v is not None},
            params=params,
            headers=headers,
        )

    def _run_job(
        self,
        job_cls: Type[T_ContextualizationJob],
        job_path: str,
        status_path: str = None,
        headers: Dict[str, Any] = None,
        **kwargs: Any,
    ) -> T_ContextualizationJob:
        if status_path is None:
            status_path = job_path + "/"
        return job_cls._load_with_status(
            self._camel_post(job_path, json=kwargs, headers=headers).json(),
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )

    @staticmethod
    def _process_file_ids(ids: Union[Sequence[int], int, None], external_ids: Union[Sequence[str], str, None]) -> List:
        if external_ids is None and ids is None:
            raise ValueError("No ids specified")

        if isinstance(ids, numbers.Integral):
            ids = [ids]
        elif isinstance(ids, list) or ids is None:
            ids = ids or []
        else:
            raise TypeError("ids must be int or list of int")

        if isinstance(external_ids, str):
            external_ids = [external_ids]
        elif isinstance(external_ids, list) or external_ids is None:
            external_ids = external_ids or []
        else:
            raise TypeError("external_ids must be str or list of str")

        id_objs = [{"fileId": id} for id in ids]
        external_id_objs = [{"fileExternalId": external_id} for external_id in external_ids]
        return [*id_objs, *external_id_objs]

    def detect(
        self,
        entities: Sequence[Union[dict, CogniteResource]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        file_ids: Union[int, Sequence[int]] = None,
        file_external_ids: Union[str, Sequence[str]] = None,
    ) -> DiagramDetectResults:
        """Detect entities in a PNID.
        The results are not written to CDF.
        Note: All users on this CDF subscription with assets read-all and files read-all capabilities in the project,
        are able to access the data sent to this endpoint.

        Args:
            entities (Sequence[Union[dict,CogniteResource]]): List of entities to detect
            search_field (str): If entities is a list of dictionaries, this is the key to the values to detect in the PnId
            partial_match (bool): Allow for a partial match (e.g. missing prefix).
            min_tokens (int): Minimal number of tokens a match must be based on
            file_ids (Sequence[int]): ID of the files, should already be uploaded in the same tenant.
            file_external_ids (Sequence[str]): File external ids.
        Returns:
            DiagramDetectResults: Resulting queued job. Note that .result property of this job will block waiting for results."""

        entities = [
            entity.dump(camel_case=True) if isinstance(entity, CogniteResource) else entity for entity in entities
        ]

        return self._run_job(
            job_path="/detect",
            status_path="/detect/",
            items=self._process_file_ids(file_ids, file_external_ids),
            entities=entities,
            partial_match=partial_match,
            search_field=search_field,
            min_tokens=min_tokens,
            job_cls=DiagramDetectResults,
        )

    @staticmethod
    def _process_detect_job(detect_job: DiagramDetectResults) -> list:
        """process the result from detect job so it complies with diagram convert schema

        Args:
            detect_job (DiagramDetectResults): detect job

        Returns:
            items: the format complies with diagram convert schema
        """
        items = [
            {k: v for k, v in item.items() if k in {"annotations", "fileId"}} for item in detect_job.result["items"]
        ]  # diagram detect always return file id.
        return items

    def convert(self, detect_job: DiagramDetectResults) -> DiagramConvertResults:
        """Convert a P&ID to interactive SVGs where the provided annotations are highlighted.

        Args:
            detect_job(DiagramConvertResults): detect job

        Returns:
            DiagramConvertResults: Resulting queued job. Note that .result property of this job will block waiting for results.
        """
        return self._run_job(
            job_path="/convert",
            status_path="/convert/",
            items=self._process_detect_job(detect_job),
            job_cls=DiagramConvertResults,
        )
