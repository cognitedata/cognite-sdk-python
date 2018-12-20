class CogniteResponse:
    """Cognite Response

    This class provides a skeleton for all data objects in this module. All response objects should inherit
    this class.
    """

    def __init__(self, internal_representation):
        self.internal_representation = internal_representation

    def __str__(self):
        return self.internal_representation["data"]["items"]

    def next_cursor(self):
        """Returns next cursor to use for paging through results"""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("nextCursor")

    def previous_cursor(self):
        """Returns previous cursor"""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("previousCursor")
