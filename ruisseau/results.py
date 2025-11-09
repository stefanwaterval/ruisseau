from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TaskResult:
    """Immutable descriptor for the outcome of a single tas run.
    Contains its id, status, and optional message.
    """

    id: str
    status: str
    message: str | None = None

    def __post_init__(self):
        if self.id == "":
            raise ValueError(f"Invalid taks id {self.id!r}: cannot be empty")
        if self.status not in ["pass", "fail"]:
            raise ValueError(f"Invalid status {self.status}: should be pass or fail")


@dataclass(frozen=True)
class DAGResult:
    """Immutable descriptor for the outcome of one DAG execution."""

    name: str
    results: list[TaskResult]
    overall: str = field(init=False)

    def __post_init__(self):
        if not isinstance(self.name, str) or self.name == "":
            raise ValueError("Invalid DAG name: cannot be empty")

        overall = "pass"
        for task_result in self.results:
            if not isinstance(task_result, TaskResult):
                raise TypeError(
                    f"Task result for '{task_result}' must be a TaskResult."
                )
            if task_result.status == "fail":
                overall = "fail"
                break

        object.__setattr__(self, "overall", overall)

    def to_dict(self) -> dict[str, Any]:
        return dict(
            name=self.name,
            overall=self.overall,
            results=[
                dict(
                    id=task_result.id,
                    status=task_result.status,
                    message=task_result.message,
                )
                for task_result in self.results
            ],
        )
