from dataclasses import dataclass


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
    tasks: dict[str, TaskResult]

    def __post_init__(self):
        if not isinstance(self.name, str) or self.name == "":
            raise ValueError("Invalid DAG name: cannot be empty")

        overall = "pass"
        for k, v in self.tasks.items():
            if not isinstance(k, str) or not k:
                raise TypeError("Task id keys must be non-empty strings")
            if not isinstance(v, TaskResult):
                raise TypeError(f"Task result for '{k}' must be a TaskResult.")
            if v.status == "fail":
                overall = "fail"
                break

        object.__setattr__(self, "overall", overall)
