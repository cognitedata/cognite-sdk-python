from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import Answer, AnswerLanguage, Summary
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._identifier import IdentifierSequenceWithInstanceId


class AIDocumentsAPI(APIClient):
    _RESOURCE_PATH = "/ai/tools/documents"

    def summarize(
        self,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> Summary | None:
        """Summarize a document using a Large Language Model.

        Note:
            Currently only supports summarizing a single document at a time, but
            this may be extended in the future.

        Args:
            id (int | None): No description.
            external_id (str | None): No description.
            instance_id (NodeId | None): No description.

        Returns:
            Summary | None: A summary of the document if the document exists else None.

        Examples:

            Summarize a single document with id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.ai.tools.documents.summarize(id=123)

            You can also use external ID or instance ID:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client.ai.tools.documents.summarize(
                ...     instance_id=NodeId("my-space", "my-xid")
                ... )
        """
        ident = IdentifierSequenceWithInstanceId.load(id, external_id, instance_id).as_singleton()
        try:
            res = self._post(self._RESOURCE_PATH + "/summarize", json={"items": ident.as_dicts()})
            return Summary._load(res.json()["items"][0])
        except CogniteAPIError as e:
            if e.code != 404:
                raise
            return None

    def ask_question(
        self,
        question: str,
        *,
        id: int | None = None,
        external_id: str | Sequence[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        language: AnswerLanguage
        | Literal[
            "Chinese",
            "Dutch",
            "English",
            "French",
            "German",
            "Italian",
            "Japanese",
            "Korean",
            "Latvian",
            "Norwegian",
            "Portuguese",
            "Spanish",
            "Swedish",
        ] = AnswerLanguage.English,
        additional_context: str | None = None,
        ignore_unknown_ids: bool = False,
    ) -> Answer:
        """Ask a question about one or more documents using a Large Language Model.

        Supports up to 100 documents at a time.

        Args:
            question (str): The question.
            id (int | None): No description.
            external_id (str | Sequence[str] | None): No description.
            instance_id (NodeId | Sequence[NodeId] | None): No description.
            language (AnswerLanguage | Literal['Chinese', 'Dutch', 'English', 'French', 'German', 'Italian', 'Japanese', 'Korean', 'Latvian', 'Norwegian', 'Portuguese', 'Spanish', 'Swedish']): The desired language of the answer, defaults to English.
            additional_context (str | None): Additional context that you want the LLM to take into account.
            ignore_unknown_ids (bool): Whether to skip documents that do not exists or that are not fully processed, instead of throwing an error

        Returns:
            Answer: The answer to the question in the form of a list of multiple content objects, each consisting of a chunk of text along with a set of references.

        Examples:

            Ask a question about a single document with id=123 and get the answer in English (default):

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.ai.tools.documents.ask(
                ...     question="What model pump was used?",
                ...     ids=123,
                ... )

            Ask a question about multiple documents referenced using external IDs, and instance ID
            and get the answer in German:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from cognite.client.data_classes.ai import AnswerLanguage
                >>> client.ai.tools.documents.ask_question(
                ...     question="What other pumps are available?",
                ...     external_ids=["foo", "bar"],
                ...     instance_id=NodeId("my-space", "my-xid"),
                ...     language=AnswerLanguage.German,
                ... )
        """
        identifiers = IdentifierSequenceWithInstanceId.load(id, external_id, instance_id)
        if len(identifiers) > 100:
            raise ValueError(f"Maximum number of documents is 100 (was {len(identifiers)})")

        body = {
            "question": question,
            "fileIds": identifiers.as_dicts(),
            "ignoreUnknownIds": ignore_unknown_ids,
            "additionalContext": additional_context,
        }
        match language:
            case AnswerLanguage():
                body["language"] = language.value
            case str():
                try:
                    body["language"] = AnswerLanguage(language.capitalize()).value
                except ValueError:
                    # Probably an unknown language, but we let the API handle it (future-proofing)
                    body["language"] = language

        response = self._post(self._RESOURCE_PATH + "/ask", json=body)
        return Answer._load(response.json()["content"])
