from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes.data_modeling import NodeId

if TYPE_CHECKING:
    pass


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
        id (int | None): The id of the document
        external_id (str | None): The external id of the document
        instance_id (NodeId| None): The instance id of the document
    """

    summary: str
    id: int | None = None
    external_id: str | None = None
    instance_id: NodeId | None = None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            summary=data["summary"],
            id=data.get("id"),
            external_id=data.get("externalId"),
            instance_id=NodeId.load(data["instanceId"]) if "instanceId" in data else None,
        )


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
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            page_number=data["pageNumber"],
            left=data["left"],
            right=data["right"],
            top=data["top"],
            bottom=data["bottom"],
        )

    def __hash__(self) -> int:
        # Hashing floats, what can go wrong? :)
        return hash((self.page_number, self.left, self.right, self.top, self.bottom))


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
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            file_id=data["fileId"],
            external_id=data.get("externalId"),
            instance_id=NodeId.load(data["instanceId"]) if "instanceId" in data else None,
            file_name=data["fileName"],
            locations=[AnswerLocation._load(d) for d in data.get("locations", [])],
        )

    def __hash__(self) -> int:
        return hash((self.file_id, self.external_id, self.instance_id, self.file_name, tuple(self.locations)))


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
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            text=data["text"],
            references=[AnswerReference._load(ref) for ref in data.get("references", [])],
        )


@dataclass
class Answer:
    """
    An answer returned from the Document Question Answering API.

    The answer is essentially a list of content objects. Each content object
    consists of a chunk of text along with a set of references.

    Each reference contains information about which document this part
    of the answer was found in, and gives the bounding box and page number
    of the piece of text the answer was constructed from.

    Args:
        content (list[AnswerContent]): The list of content objects.
    """

    content: list[AnswerContent]

    def __str__(self) -> str:
        return f"Answer({self.full_answer!r})"

    def _repr_html_(self) -> str:
        return str(self)

    @property
    def full_answer(self) -> str:
        """
        Get the full answer text. This is the concatenation of the texts from
        all the content objects.
        """
        return "".join(cnt.text for cnt in self.content)

    @property
    def all_references(self) -> set[AnswerReference]:
        """
        Get all unique references. This is the full set of references from
        all the content objects.
        """
        return set().union(*(cnt.references for cnt in self.content))

    @classmethod
    def _load(cls, data: list[dict[str, Any]]) -> Self:
        return cls(content=[AnswerContent._load(cnt) for cnt in data])
