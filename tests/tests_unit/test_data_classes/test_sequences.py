import pytest

from cognite.client.data_classes import SequenceColumn, SequenceColumnList, SequenceRow, SequenceRows, SequenceRowsList


class TestSequenceRowsList:
    @pytest.mark.dsl
    def test_sequence_notebook_repr_html(self):
        sequence_rows_list = SequenceRowsList(
            [
                SequenceRows(
                    rows=[SequenceRow(row_number=0, values=[1, 2, 3])],
                    columns=SequenceColumnList([SequenceColumn(col) for col in ["co1", "col2", "col3"]]),
                    external_id="external_id",
                )
            ]
        )

        html_repr = sequence_rows_list._repr_html_()

        assert isinstance(html_repr, str)
