import pytest

from cognite.client.data_classes.contextualization import DiagramConvertResults, DiagramDetectItem, DiagramDetectResults

PNID_FILE_ID = 3261066797848581


class TestPNIDParsingIntegration:
    @pytest.mark.skip(reason="fails consistently")
    def test_run_diagram_detect(self, cognite_client):
        entities = [{"name": "YT-96122"}, {"name": "XE-96125", "ee": 123}, {"name": "XWDW-9615"}]
        file_id = PNID_FILE_ID
        job = cognite_client.diagrams.detect(file_ids=[file_id], entities=entities)
        assert isinstance(job, DiagramDetectResults)
        assert {"statusCount", "numFiles", "items", "partialMatch", "minTokens", "searchField"}.issubset(job.result)
        assert {"fileId", "annotations"}.issubset(job.result["items"][0])
        assert "Completed" == job.status
        assert [] == job.errors
        assert isinstance(job.items[0], DiagramDetectItem)
        assert isinstance(job[PNID_FILE_ID], DiagramDetectItem)

        assert 2 == len(job[PNID_FILE_ID].annotations)
        for annotation in job[PNID_FILE_ID].annotations:
            assert 1 == annotation["region"]["page"]

        convert_job = job.convert()

        assert isinstance(convert_job, DiagramConvertResults)
        assert {"items", "grayscale", "statusCount", "numFiles"}.issubset(convert_job.result)
        assert {"results", "fileId"}.issubset(convert_job.result["items"][0])
        assert {"pngUrl", "svgUrl", "page"}.issubset(convert_job.result["items"][0]["results"][0])
        assert "Completed" == convert_job.status

        for res_page in convert_job[PNID_FILE_ID].pages:
            assert 1 == res_page.page
            assert ".svg" in res_page.svg_url
            assert ".png" in res_page.png_url
