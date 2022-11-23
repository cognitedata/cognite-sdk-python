from typing import Any, Dict, List, Optional, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.contextualization import DiagramDetectResults, FileReference


class DiagramFile:
    def __init__(self, file_id: int):
        self.file_id: int = file_id
        self.page_count: Optional[int] = None
        self.file_jobs: List[DiagramRangeJob] = []

    def make_range_jobs(self, batch_size: int) -> None:
        if self.page_count is None and self.file_jobs == []:
            self.file_jobs.append(DiagramRangeJob(first_page=1, last_page=batch_size, diagram_file=self))
            return
        if self.page_count is not None and len(self.file_jobs) == 1:
            self.file_jobs.extend(
                [
                    DiagramRangeJob(first_page=first_page, last_page=first_page + batch_size - 1, diagram_file=self)
                    for first_page in range(batch_size + 1, self.page_count + 1, batch_size)
                ]
            )
            return
        raise Exception("Logical flaw in diagram processor")


class DiagramRangeJob:
    def __init__(
        self, first_page: int, last_page: int, diagram_file: DiagramFile, job_id: Optional[int] = None, stage: int = 0
    ):
        self.diagram_file = diagram_file
        self.job_id: Optional[int] = job_id
        self.first_page = first_page
        self.last_page = last_page
        self.stage: int = stage

    @property
    def page_range(self) -> Dict[str, int]:
        return {"begin": self.first_page, "end": self.last_page}


class DiagramProcessor:
    def __init__(
        self,
        client: CogniteClient,
        diagram_files: List[DiagramFile],
        entities: List[Union[Dict, CogniteResource]],
        search_field: str = "name",
        partial_match: bool = False,
        min_tokens: int = 2,
        max_concurrent_jobs: int = 1000,
        batch_size: int = 50,
        pages_per_request: int = 50,
        running_jobs: Optional[Dict[int, List[Tuple[int, int]]]] = None,
        unprocessed_results: Optional[Dict[int, List[Tuple[int, int]]]] = None,
        processed_jobs: Optional[Dict[int, List[Tuple[int, int]]]] = None,
    ):
        self.client = client
        self.diagram_files = diagram_files
        self.entities = [e.dump() if isinstance(e, CogniteResource) else e for e in entities]
        self.search_field = search_field
        self.partial_match = partial_match
        self.min_tokens = min_tokens
        self.max_concurrent_jobs = max_concurrent_jobs
        self.batch_size = batch_size
        self.pages_per_request = pages_per_request

        self.sub_job_indices: List[Tuple[int, int]] = []
        self.running_jobs = running_jobs or {}
        self.unprocessed_results = unprocessed_results or {}
        self.processed_jobs = processed_jobs or {}

        self.create_first_jobs()

    def create_first_jobs(self) -> None:
        for diagram_file in self.diagram_files:
            if len(diagram_file.file_jobs) == 0:
                diagram_file.make_range_jobs(self.batch_size)

    def jobs_by_status(self, status: int) -> List[DiagramRangeJob]:
        return [
            file_job
            for diagram_file in self.diagram_files
            for file_job in diagram_file.file_jobs
            if file_job.stage == status
        ]

    def update_job_statuses(self) -> None:
        statuses: List[str] = []  # Todo: Implement
        running_job_ids = [job_id for job_id in self.running_jobs.keys()]
        for running, status in zip(running_job_ids, statuses):
            if status == "Completed":
                for (file_index, range_index) in self.running_jobs[running]:
                    diagram_file = self.diagram_files[file_index]
                    range_job = diagram_file.file_jobs[range_index]
                    range_job.stage = 2
            self.unprocessed_results[running] = self.running_jobs[running]
            self.running_jobs.pop(running)

    def get_results(self) -> Optional[Dict[str, Any]]:
        if len(self.unprocessed_results) == 0:
            return None
        job_id, range_job_indices = self.unprocessed_results.popitem()
        self.processed_jobs[job_id] = range_job_indices

        job = DiagramDetectResults(job_id=job_id, client=self.client)
        result = job.result
        for (file_index, range_index) in range_job_indices:
            diagram_file = self.diagram_files[file_index]
            range_job = diagram_file.file_jobs[range_index]
            range_job.stage = 3

            relevant_results = [
                r
                for r in result["items"]
                if r["file_id"] == diagram_file.file_id and r["page_range"] == range_job.page_range
            ]
            if len(relevant_results) == 0:  # For now the timeout case
                continue

            relevant_result = relevant_results[0]  # Can only be one
            if range_job.diagram_file.page_count is None:
                diagram_file.page_count = relevant_result["page_count"]
                diagram_file.make_range_jobs(self.batch_size)
        return result

    def post_more_jobs(self) -> None:
        number_of_running_file_jobs = sum(1 for job_list in self.running_jobs.values() for item in job_list)
        capacity = self.max_concurrent_jobs - number_of_running_file_jobs
        if capacity == 0:
            return
        todo_list = [
            (i, j)
            for i, diagram_file in enumerate(self.diagram_files)
            for j, range_job in enumerate(diagram_file.file_jobs)
            if range_job.stage == 0
        ]

        for i in range(0, min(capacity, len(todo_list)), self.batch_size):
            batch = todo_list[i : min(i + self.batch_size, capacity, len(todo_list))]
            job = self.client.diagrams.detect(
                entities=self.entities,
                search_field=self.search_field,
                partial_match=self.partial_match,
                min_tokens=self.min_tokens,
                file_references=[
                    FileReference(
                        file_id=self.diagram_files[file_index].file_id,
                        first_page=self.diagram_files[file_index].file_jobs[range_index].first_page,
                        last_page=self.diagram_files[file_index].file_jobs[range_index].last_page,
                    )
                    for (file_index, range_index) in batch
                ],
            )
            assert job.job_id is not None  # My oh my mypy
            self.running_jobs[job.job_id] = batch
            for (file_index, range_index) in batch:
                self.diagram_files[file_index].file_jobs[range_index].stage = 1
