# -*- coding: utf-8 -*-
import pandas as pd

from cognite.client._api_client import APIClient, CogniteResponse


class TagMatchingResponse(CogniteResponse):
    """Tag Matching Response Object.

    In addition to the standard output formats this data object also has a to_list() method which returns a list of
    names of the tag matches.
    """

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        matches = []
        for tag in self.internal_representation["data"]["items"]:
            for match in tag["matches"]:
                matches.append(
                    {
                        "tag": tag["tagId"],
                        "match": match["tagId"],
                        "score": match["score"],
                        "platform": match["platform"],
                    }
                )
        if matches:
            return pd.DataFrame(matches)[["tag", "match", "platform", "score"]]
        return pd.DataFrame()

    def to_list(self, first_matches_only=True):
        """Returns a list representation of the matches.

        Args:
            first_matches_only (bool):      Boolean determining whether or not to return only the top match for each
                                            tag.

        Returns:
            list: list of matched tags.
        """
        if self.to_pandas().empty:
            return []
        if first_matches_only:
            return self.to_pandas().sort_values(["score", "match"]).groupby(["tag"]).first()["match"].tolist()
        return self.to_pandas().sort_values(["score", "match"])["match"].tolist()


class TagMatchingClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.5", **kwargs)

    def tag_matching(self, tag_ids, fuzzy_threshold=0, platform=None) -> TagMatchingResponse:
        """Returns a TagMatchingObject containing a list of matched tags for the given query.

        This method takes an arbitrary string as argument and performs fuzzy matching with a user defined threshold
        toward tag ids in the system.

        Args:
            tag_ids (list):         The tag_ids to retrieve matches for.

            fuzzy_threshold (int):  The threshold to use when searching for matches. A fuzzy threshold of 0 means you only
                                    want to accept perfect matches. Must be >= 0.

            platform (str):         The platform to search on.

        Returns:
            stable.tagmatching.TagMatchingResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        url = "/tagmatching"
        body = {"tagIds": tag_ids, "metadata": {"fuzzyThreshold": fuzzy_threshold, "platform": platform}}
        res = self._post(url=url, body=body)
        return TagMatchingResponse(res.json())
