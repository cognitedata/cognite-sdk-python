class CogniteResponse:
    """Cognite Response

    All responses inherit from this class.
    """

    def __init__(self, internal_representation):
        self.internal_representation = internal_representation

    def __str__(self):
        data = self.internal_representation.get("data", {})
        if "items" in data:
            return data["items"]
        return data

    def next_cursor(self):
        """Returns next cursor to use for paging through results"""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("nextCursor")

    def previous_cursor(self):
        """Returns previous cursor"""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("previousCursor")
