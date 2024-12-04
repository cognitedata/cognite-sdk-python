from __future__ import annotations

from collections.abc import Sequence

from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import Answer, AnswerLanguage, Summary
from cognite.client.utils._identifier import InstanceId
from cognite.client.utils.useful_types import SequenceNotStr


class AIDocumentsAPI(APIClient):
    _RESOURCE_PATH = "/ai/tools/documents"

    def summarize(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[InstanceId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> list[Summary]:
        """Summarize a document using a Large Language Model.

        Currently only supports summarizing a single document at a time, but
        this may be extended in the future.

        Args:
            ids (Sequence[int] | None): Internal ids of documents to summarize.
            external_ids (SequenceNotStr[str] | None): External ids of documents to summarize.
            instance_ids (Sequence[InstanceId] | None): Instance ids of documents to summarize.
            ignore_unknown_ids (bool): Whether to skip documents that can't be summarized, without throwing an error

        Returns:
            list[Summary]: No description.
        Examples:

            Summarize a single document with internal id 123

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.ai.tools.documents.summarize(ids=[123])
        """
        body = {
            "items": (
                [{"id": id} for id in ids or []]
                + [{"externalId": external_id} for external_id in external_ids or []]
                + [{"instanceId": instance_id.dump()} for instance_id in instance_ids or []]
            ),
            "ignoreUnknownIds": ignore_unknown_ids,
        }

        response = self._post(self._RESOURCE_PATH + "/summarize", json=body)
        response_json = response.json()

        summaries = []
        for item in response_json["items"]:
            instance_id = InstanceId.load(item["instanceId"]) if "instanceId" in item else None
            summaries.append(
                Summary(
                    id=item.get("id"),
                    external_id=item.get("externalId"),
                    instance_id=instance_id,
                    summary=item["summary"],
                )
            )

        return summaries

    def ask_question(
        self,
        question: str,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[InstanceId] | None = None,
        language: AnswerLanguage = AnswerLanguage.English,
        ignore_unknown_ids: bool = False,
        additional_context: str | None = None,
    ) -> Answer:
        """Ask a question about one or more documents using a Large Language Model.

        Supports up to 100 documents at a time.

        Args:
            question (str): The question.
            ids (Sequence[int] | None): Internal ids of documents to find the answer in.
            external_ids (SequenceNotStr[str] | None): External ids of documents to find the answer in.
            instance_ids (Sequence[InstanceId] | None): Instance ids of documents to find the answer in.
            language (AnswerLanguage): The desired language of the answer
            ignore_unknown_ids (bool): Whether to skip documents that are not fully processed, without throwing an error
            additional_context (str | None): Additional context that you want the LLM to take into account.

        Returns:
            Answer: No description.
        Examples:

            Ask a question about a single document with internal id 123

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.ai.tools.documents.ask(question="What model pump was used?", ids=[123])
        """
        body = {
            "question": question,
            "fileIds": (
                [{"id": id} for id in ids or []]
                + [{"externalId": external_id} for external_id in external_ids or []]
                + [{"instanceId": instance_id.dump()} for instance_id in instance_ids or []]
            ),
            "language": language.value,
            "ignoreUnknownIds": ignore_unknown_ids,
            "additionalContext": additional_context,
        }

        response = self._post(self._RESOURCE_PATH + "/ask", json=body)
        return Answer.load(response.json())
