import pytest

from cognite._utils import APIError
from cognite.v06 import sequences
from cognite.v06.dto import Sequence, Column, SequenceDataPost, Row, RowValue, SequenceDataRequest, SequenceDataResponse

# This variable will hold the ID of the sequence that is created in one of the test fixtures of this class.
CREATED_SEQUENCE_ID: int = None
# This variable holds the external id used for the sequence that'll be created (and deleted) in these tests
SEQUENCE_EXTERNAL_ID: str = "external_id"


class TestSequences:

    @pytest.fixture(scope="class")
    def sequence_that_isnt_created(
            self
    ):
        """Returns a Sequence that hasn't been created yet. (It does not have an ID)"""
        global SEQUENCE_EXTERNAL_ID

        column: Column = Column(
            id=None,
            name="test_column",
            externalId="external_id",
            dataType="STRING",
            metadata={}
        )
        return Sequence(
            id=None,
            name="test_sequence",
            externalId=SEQUENCE_EXTERNAL_ID,
            assetId=None,
            columns=[column],
            description="Test sequence",
            metadata={}
        )

    @pytest.fixture(scope="class")
    def sequence_that_is_created_retrieved_by_id(
            self,
            sequence_that_isnt_created
    ):
        """Returns the created sequence by using the cognite id"""
        global CREATED_SEQUENCE_ID
        if CREATED_SEQUENCE_ID:
            return sequences.get_sequence_by_id(CREATED_SEQUENCE_ID)
        else:
            # Create the sequence
            created_sequence: Sequence = sequences.post_sequences([sequence_that_isnt_created])
            # Store the id of the created sequence
            CREATED_SEQUENCE_ID = created_sequence.id
            return created_sequence

    @pytest.fixture(scope="class")
    def sequence_that_is_created_retrieved_by_external_id(
            self,
            sequence_that_isnt_created
    ):
        """Returns the created sequence by using the external id"""
        global CREATED_SEQUENCE_ID, SEQUENCE_EXTERNAL_ID
        if CREATED_SEQUENCE_ID:
            return sequences.get_sequence_by_external_id(SEQUENCE_EXTERNAL_ID)
        else:
            # Create the sequence
            created_sequence: Sequence = sequences.post_sequences([sequence_that_isnt_created])
            # Store the id of the created sequence
            CREATED_SEQUENCE_ID = created_sequence.id
            return created_sequence

    def test_get_sequence_by_id(
            self,
            sequence_that_is_created_retrieved_by_id,
            sequence_that_isnt_created
    ):
        global CREATED_SEQUENCE_ID
        assert isinstance(sequence_that_is_created_retrieved_by_id, Sequence)
        assert sequence_that_is_created_retrieved_by_id.id == CREATED_SEQUENCE_ID
        assert sequence_that_is_created_retrieved_by_id.name == sequence_that_isnt_created.name

    def test_get_sequence_by_external_id(
            self,
            sequence_that_is_created_retrieved_by_external_id,
            sequence_that_isnt_created
    ):
        global CREATED_SEQUENCE_ID
        assert isinstance(sequence_that_is_created_retrieved_by_external_id, Sequence)
        assert sequence_that_is_created_retrieved_by_external_id.id == CREATED_SEQUENCE_ID
        assert sequence_that_is_created_retrieved_by_external_id.name == sequence_that_isnt_created.name

    def test_post_data_to_sequence_and_get_data_from_sequence(
            self,
            sequence_that_is_created_retrieved_by_id
    ):
        # Prepare some data to post
        sequenceData: SequenceDataPost = SequenceDataPost(
            id=sequence_that_is_created_retrieved_by_id.id,
            rows=[
                Row(
                    rowNumber=1,
                    values=[
                        RowValue(
                            columnId=sequence_that_is_created_retrieved_by_id.columns[0].id,
                            value="42"
                        )
                    ]
                )
            ]
        )
        # Post data
        res = sequences.post_data_to_sequence(
            sequenceData=sequenceData
        )
        assert res == {}
        # Sleep a little, to give the api a chance to process the data
        import time
        time.sleep(5)
        # Get the data
        sequenceDataResponse: SequenceDataResponse = sequences.get_data_from_sequence(
            id=sequence_that_is_created_retrieved_by_id.id,
            sequenceDataRequest=SequenceDataRequest(
                inclusiveFrom=1,
                inclusiveTo=1,
                limit=1,
                columnIds=[
                    sequence_that_is_created_retrieved_by_id.columns[0].id
                ]
            )
        )
        # Verify that the data is the same
        assert sequenceData.rows[0].rowNumber == sequenceDataResponse.rows[0].rowNumber
        assert sequenceData.rows[0].values[0].columnId == sequenceDataResponse.rows[0].values[0].columnId
        assert sequenceData.rows[0].values[0].value == sequenceDataResponse.rows[0].values[0].value

    def test_delete_sequence_by_id(self, sequence_that_is_created_retrieved_by_id):
        sequences.delete_sequence_by_id(sequence_that_is_created_retrieved_by_id.id)
        # Check that we now can't fetch it
        with pytest.raises(APIError):
            sequences.get_sequence_by_id(sequence_that_is_created_retrieved_by_id.id)
