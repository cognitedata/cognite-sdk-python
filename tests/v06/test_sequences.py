import pytest

from cognite._utils import APIError
from cognite.v06 import sequences
from cognite.v06.dto import Sequence, Column

# This variable will hold the ID of the sequence that is created in one of the test fixtures of this class.
CREATED_SEQUENCE_ID: int = None


class TestSequences:

    @pytest.fixture(scope="class")
    def sequence_that_isnt_created(self):
        """Returns a Sequence that hasn't been created yet. (It does not have an ID)"""
        column: Column = Column(
            id=None,
            name="test_column",
            external_id="external_id",
            data_type="STRING",
            metadata={}
        )
        return Sequence(
            id=None,
            name="test_sequence",
            external_id="external_id",
            asset_id=None,
            columns=[column],
            description="Test sequence",
            metadata={}
        )

    @pytest.fixture(scope="class")
    def sequence_that_is_created(self, sequence_that_isnt_created):
        """Returns a created sequence. (It has an ID)"""
        global CREATED_SEQUENCE_ID
        if CREATED_SEQUENCE_ID:
            return sequences.get_sequence_by_id(CREATED_SEQUENCE_ID)
        else:
            # Create the sequence
            created_sequence: Sequence = sequences.post_sequences([sequence_that_isnt_created])
            # Store the id of the created sequence
            CREATED_SEQUENCE_ID = created_sequence.id
            return created_sequence

    def test_get_sequence_by_id(self, sequence_that_is_created, sequence_that_isnt_created):
        global CREATED_SEQUENCE_ID
        assert isinstance(sequence_that_is_created, Sequence)
        assert sequence_that_is_created.id == CREATED_SEQUENCE_ID
        assert sequence_that_is_created.name == sequence_that_isnt_created.name

    def test_delete_sequence_by_id(self, sequence_that_is_created):
        sequences.delete_sequence_by_id(sequence_that_is_created.id)
        # Check that we now can't fetch it
        with pytest.raises(APIError):
            sequences.get_sequence_by_id(sequence_that_is_created.id)
