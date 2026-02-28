"""
===============================================================================
c5fabf1e47675fc722ffaa889e3ce86b
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import Answer, AnswerLanguage, Summary
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._async_helpers import run_sync


class SyncAIDocumentsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def summarize(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> Summary:
        """
        `Summarize a document using a Large Language Model. <https://api-docs.cognite.com/20230101/tag/Document-AI/operation/document_questioning_ai_tools_documents_ask_post>`_

        Note:
            Currently only supports summarizing a single document at a time, but
            this may be extended in the future.

        Args:
            id (int | None): The ID of the document
            external_id (str | None): The external ID of the document
            instance_id (NodeId | None): The instance ID of the document

        Returns:
            Summary: A summary of the document.

        Examples:

            Summarize a single document using ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.ai.tools.documents.summarize(id=123)

            You can also use external ID or instance ID:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client.ai.tools.documents.summarize(
                ...     instance_id=NodeId("my-space", "my-xid")
                ... )
        """
        return run_sync(
            self.__async_client.ai.tools.documents.summarize(id=id, external_id=external_id, instance_id=instance_id)
        )

    def ask_question(
        self,
        question: str,
        *,
        id: int | Sequence[int] | None = None,
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
        """
        `Ask a question about one or more documents using a Large Language Model. <https://api-docs.cognite.com/20230101/tag/Document-AI/operation/documents_summary_api_v1_projects__projectName__ai_tools_documents_summarize_post>`_

        Supports up to 100 documents at a time.

        Args:
            question (str): The question.
            id (int | Sequence[int] | None): The ID(s) of the document(s)
            external_id (str | Sequence[str] | None): The external ID(s) of the document(s)
            instance_id (NodeId | Sequence[NodeId] | None): The instance ID(s) of the document(s)
            language (AnswerLanguage | Literal['Chinese', 'Dutch', 'English', 'French', 'German', 'Italian', 'Japanese', 'Korean', 'Latvian', 'Norwegian', 'Portuguese', 'Spanish', 'Swedish']): The desired language of the answer, defaults to English.
            additional_context (str | None): Additional context that you want the LLM to take into account.
            ignore_unknown_ids (bool): Whether to skip documents that do not exist or that are not fully processed, instead of throwing an error. If no valid documents are found, an error will always be raised.

        Returns:
            Answer: The answer to the question in the form of a list of multiple content objects, each consisting of a chunk of text along with a set of references.

        Examples:

            Ask a question about a single document with id=123 and get the answer in English (default):

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.ai.tools.documents.ask_question(
                ...     question="What model pump was used?",
                ...     id=123,
                ... )

            Ask a question about multiple documents referenced using external IDs, and instance ID
            and get the answer in German:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from cognite.client.data_classes.ai import AnswerLanguage
                >>> client.ai.tools.documents.ask_question(
                ...     question="What other pumps are available?",
                ...     external_id=["foo", "bar"],
                ...     instance_id=NodeId("my-space", "my-xid"),
                ...     language=AnswerLanguage.German,
                ... )
        """
        return run_sync(
            self.__async_client.ai.tools.documents.ask_question(
                question=question,
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                language=language,
                additional_context=additional_context,
                ignore_unknown_ids=ignore_unknown_ids,
            )
        )
