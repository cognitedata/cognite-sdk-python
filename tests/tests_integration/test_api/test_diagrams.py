from collections import defaultdict

import pytest

from cognite.client.data_classes.contextualization import (
    DetectJobBundle,
    DiagramConvertResults,
    DiagramDetectItem,
    DiagramDetectResults,
    FileReference,
)

PNID_FILE_ID = 3261066797848581

ELEVEN_PAGE_PNID_EXTERNAL_ID = "functional_tests.pdf"
FIFTY_FIVE_PAGE_PNID_EXTERNAL_ID = "5functional_tests.pdf"


class TestPNIDParsingIntegration:
    @pytest.mark.skip
    def test_run_diagram_detect(self, cognite_client):
        entities = [{"name": "YT-96122"}, {"name": "XE-96125", "ee": 123}, {"name": "XWDW-9615"}]
        file_id = PNID_FILE_ID
        detect_job = cognite_client.diagrams.detect(file_ids=[file_id], entities=entities)
        assert isinstance(detect_job, DiagramDetectResults)
        assert {"statusCount", "numFiles", "items", "partialMatch", "minTokens", "searchField"}.issubset(
            detect_job.result
        )
        assert {"fileId", "annotations"}.issubset(detect_job.result["items"][0])
        assert "Completed" == detect_job.status
        assert [] == detect_job.errors
        assert isinstance(detect_job.items[0], DiagramDetectItem)
        assert isinstance(detect_job[PNID_FILE_ID], DiagramDetectItem)

        assert 3 == len(detect_job[PNID_FILE_ID].annotations)
        for annotation in detect_job[PNID_FILE_ID].annotations:
            assert 1 == annotation["region"]["page"]

        convert_job = detect_job.convert()

        assert isinstance(convert_job, DiagramConvertResults)
        assert {"items", "grayscale", "statusCount", "numFiles"}.issubset(convert_job.result)
        assert {"results", "fileId"}.issubset(convert_job.result["items"][0])
        assert {"pngUrl", "svgUrl", "page"}.issubset(convert_job.result["items"][0]["results"][0])
        assert "Completed" == convert_job.status

        for res_page in convert_job[PNID_FILE_ID].pages:
            assert 1 == res_page.page
            assert ".svg" in res_page.svg_url
            assert ".png" in res_page.png_url

        # Enable multiple jobs
        job_bundle, _unposted_jobs = cognite_client.diagrams.detect(
            file_ids=[file_id], entities=entities, multiple_jobs=True
        )
        assert isinstance(job_bundle, DetectJobBundle)
        succeeded, failed = job_bundle.result
        assert succeeded[0]["status"] in ["Completed", "Running"]
        assert len(succeeded) == 1
        assert len(failed) == 0

    @pytest.mark.skip
    def test_run_diagram_detect_with_page_range(self, cognite_client):
        entities = [{"name": "PH-ME-P-0156-001", "id": 1}, {"name": "PH-ME-P-0156-002", "id": 2}]
        # References to the above are expected on page 6 and page 11, and repeating every 11 pages.

        detected = cognite_client.diagrams.detect(
            file_references=[
                FileReference(file_external_id=ELEVEN_PAGE_PNID_EXTERNAL_ID),
                FileReference(file_external_id=FIFTY_FIVE_PAGE_PNID_EXTERNAL_ID, first_page=1, last_page=11),
                FileReference(file_external_id=FIFTY_FIVE_PAGE_PNID_EXTERNAL_ID, first_page=45, last_page=55),
            ],
            entities=entities,
        )

        result = detected.result
        pages_with_annotations_per_subjob = [
            [annotation["region"]["page"] for annotation in result["items"][i]["annotations"]] for i in range(3)
        ]

        # Expecting 6 on each of these pages, but does not have to test every possible regression of the API in the sdk
        assert set(pages_with_annotations_per_subjob[0]) == {6, 11}
        assert set(pages_with_annotations_per_subjob[1]) == {6, 11}
        assert set(pages_with_annotations_per_subjob[2]) == {50, 55}

    def test_run_diagram_detect_in_pattern_mode(self, cognite_client):
        entities = [
            {"sample": "[PH]-ME-P-0156-001", "resourceType": "file_reference"},
            {"sample": "23-TI-92101-01", "resourceType": "instrument"},
        ]
        detected = cognite_client.diagrams.detect(
            file_references=[
                FileReference(file_external_id=ELEVEN_PAGE_PNID_EXTERNAL_ID, first_page=11, last_page=11),
            ],
            entities=entities,
            pattern_mode=True,
        )

        result = detected.result
        detected_by_resource_type = defaultdict(list)
        for annotation in result["items"][0]["annotations"]:
            detected_by_resource_type[annotation["entities"][0]["resourceType"]].append(annotation)

        assert len(detected_by_resource_type["file_reference"]) >= 10  # 14 seen when making the test
        assert len(detected_by_resource_type["instrument"]) >= 60  # 72 seen when making the test
