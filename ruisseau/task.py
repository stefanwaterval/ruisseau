from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Task:
    """Immutable descriptor for a DAG node.
    Contains an id and dependencies.
    """

    id: str
    deps: Tuple[str, ...] = ()

    def __post_init__(self):
        if self.id == "":
            raise ValueError(f"Invalid taks id {self.id!r}: cannot be empty")
        if self.id in self.deps:
            raise ValueError(f"Invalid dependecies: deps cannot contain id {self.id}")
        if "" in self.deps:
            raise ValueError("Invalid dependencies: deps cannot contain empty strings")
        if len(self.deps) != len(set(self.deps)):
            raise ValueError("Invalid dependencies: deps cannot contain duplicates")
