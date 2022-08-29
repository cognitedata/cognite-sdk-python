import numbers
from typing import Any, Dict, List, Union

from cognite.client._api_client import APIClient
from cognite.client.data_classes import ContextualizationJob
from cognite.client.utils._auxiliary import to_camel_case
from requests import Response


class ContextAPI(APIClient):
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

    def _camel_get(self, context_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None) -> Response:
        return self._get(
            self._RESOURCE_PATH + context_path,
            params={to_camel_case(k): v for k, v in (params or {}).items() if v is not None},
            headers=headers,
        )

    @staticmethod
    def _process_file_ids(ids: Union[List[int], int, None], external_ids: Union[List[str], str, None]) -> List:
        """
        Utility for sanitizing a given lists of ids and external ids.
        Returns the concatenation of the ids an external ids in the format
        expected by the Context API.
        """
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

    def _run_job(self, job_path, status_path=None, headers=None, job_cls=None, **kwargs) -> ContextualizationJob:
        job_cls = job_cls or ContextualizationJob
        if status_path is None:
            status_path = job_path + "/"
        return job_cls._load_with_status(
            self._camel_post(job_path, json=kwargs, headers=headers).json(),
            status_path=self._RESOURCE_PATH + status_path,
            cognite_client=self._cognite_client,
        )
