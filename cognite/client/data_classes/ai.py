from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from cognite.client.data_classes.data_modeling import NodeId


class AnswerLanguage(Enum):
    Chinese = "Chinese"
    Dutch = "Dutch"
    English = "English"
    French = "French"
    German = "German"
    Italian = "Italian"
    Japanese = "Japanese"
    Korean = "Korean"
    Latvian = "Latvian"
    Norwegian = "Norwegian"
    Portuguese = "Portuguese"
    Spanish = "Spanish"
    Swedish = "Swedish"


@dataclass
class Summary:
    """
    A summary object consisting of a textual summary plus the id of the summarized document

    Args:
        summary (str): The textual summary of the document
        id (int | None): The internal id of the document
        external_id (str | None): The external id of the document
        instance_id (NodeId| None): The instance id of the document
    """

    summary: str
    id: int | None = None
    external_id: str | None = None
    instance_id: NodeId | None = None


@dataclass
class AnswerLocation:
    """
    A location object

    The location object consists of a page number and a bounding box. This
    specifies exactly where inside a document an answer can be found.

    Args:
        page_number (int): Page number, starting with 1
        left (float): Leftmost edge of the bounding box
        right (float): Rightmost edge of the bounding box
        top (float): Topmost edge of the bounding box
        bottom (float): Bottommost edge of the bounding box
    """

    page_number: int
    left: float
    right: float
    top: float
    bottom: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> AnswerLocation:
        return AnswerLocation(
            page_number=data["pageNumber"],
            left=data["left"],
            right=data["right"],
            top=data["top"],
            bottom=data["bottom"],
        )


@dataclass
class AnswerReference:
    """
    A single reference object.

    The reference object specifies which file an answer is based on, and
    has a list of locations pointing to the specific pages and bounding boxes
    where the answer was found.

    Args:
        file_id (int): The internal id of the document
        external_id (str | None): The external id of the document
        instance_id (NodeId | None): The instance id of the document
        file_name (str): The name of the document
        locations (list[AnswerLocation]): A list of locations within the document, where the answer was found
    """

    file_id: int
    external_id: str | None
    instance_id: NodeId | None
    file_name: str
    locations: list[AnswerLocation]

    @classmethod
    def load(cls, data: dict[str, Any]) -> AnswerReference:
        return AnswerReference(
            file_id=data["fileId"],
            external_id=data.get("externalId"),
            instance_id=NodeId.load(data["instanceId"]) if "instanceId" in data else None,
            file_name=data["fileName"],
            locations=[AnswerLocation.load(d) for d in data.get("locations", [])],
        )


@dataclass
class AnswerContent:
    """
    A single content object.

    It consists of part of the answer from the LLM along with references to
    the documents containing the source material for the answer.

    Args:
        text (str): The extracted plain text
        content (list[AnswerReference]): The list of references
    """

    text: str
    references: list[AnswerReference]

    @classmethod
    def load(cls, data: dict[str, Any]) -> AnswerContent:
        return AnswerContent(
            text=data["text"],
            references=[AnswerReference.load(d) for d in data.get("references", [])],
        )


@dataclass
class Answer:
    """
    An answer returned from the Document Question Answering API.

    The answer contains of multiple content objects. Each content object
    consists of a chunk of text along with a set of references.

    Each reference contains information about which document this part
    of the answer was found in, and gives the bounding box and page number
    of the piece of text the answer was constructed from.

    Args:
        content (list[AnswerContent]): The list of content objects.
    """

    content: list[AnswerContent]

    @classmethod
    def load(cls, data: dict[str, Any]) -> Answer:
        return Answer(content=[AnswerContent.load(c) for c in data["content"]])
